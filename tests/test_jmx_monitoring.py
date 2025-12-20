import os
import tempfile
import types
import unittest
import time
from unittest import mock

import yaml

import jmx_monitoring as jm
from jmx_monitoring import Config, ConfigError, HealthCheck, JMXMonitor, MetricsManager


class ConfigLoadTests(unittest.TestCase):
    def setUp(self):
        self._orig_env = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._orig_env)

    def _write_cfg(self, data: dict) -> str:
        fd, path = tempfile.mkstemp(prefix="cfg_", suffix=".yml")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            yaml.safe_dump(data, f)
        return path

    def test_load_requires_endpoints(self):
        p = self._write_cfg({"jmx_items": {"m1": {"objectName": "o", "attribute": "a"}}})
        try:
            with self.assertRaises(ConfigError):
                Config.load(p)
        finally:
            os.unlink(p)

    def test_load_requires_jmx_items(self):
        p = self._write_cfg(
            {"cluster_endpoints": ["10.0.0.1"], "scrape_duration": 10, "port": 9095, "scan_new_nodes_interval": 10}
        )
        try:
            with self.assertRaises(ConfigError):
                Config.load(p)
        finally:
            os.unlink(p)

    def test_env_overrides_endpoints_and_knobs(self):
        p = self._write_cfg(
            {
                "cluster_endpoints": ["10.0.0.1"],
                "scrape_duration": 30,
                "port": 9000,
                "scan_new_nodes_interval": 45,
                "jmx_port": 7100,
                "max_workers": 5,
                "jmx_items": {"pending": {"objectName": "o", "attribute": "a"}},
            }
        )
        try:
            os.environ["CASSANDRA_CLUSTER_ENDPOINTS"] = "10.0.0.8,10.0.0.9"
            os.environ["JMX_SCRAPE_DURATION"] = "60"
            os.environ["JMX_EXPORTER_PORT"] = "9095"
            os.environ["NODE_SCAN_INTERVAL"] = "120"
            os.environ["JMX_PORT"] = "7199"
            os.environ["MAX_WORKERS"] = "20"
            os.environ["JMX_CONNECT_TIMEOUT"] = "7"
            os.environ["SHARD_TOTAL"] = "3"
            os.environ["SHARD_ID"] = "1"
            os.environ["SCRAPE_BATCH_SIZE"] = "25"
            os.environ["DISABLE_INTERNAL_METRICS"] = "true"

            cfg = Config.load(p)
            self.assertEqual(cfg.cluster_endpoints, ["10.0.0.8", "10.0.0.9"])
            self.assertEqual(cfg.scrape_duration, 60)
            self.assertEqual(cfg.port, 9095)
            self.assertEqual(cfg.scan_new_nodes_interval, 120)
            self.assertEqual(cfg.jmx_port, 7199)
            self.assertEqual(cfg.max_workers, 20)
            self.assertEqual(cfg.jmx_connect_timeout, 7)
            self.assertEqual(cfg.shard_total, 3)
            self.assertEqual(cfg.shard_id, 1)
            self.assertEqual(cfg.scrape_batch_size, 25)
            self.assertTrue(cfg.disable_internal_metrics)
        finally:
            os.unlink(p)

    def test_invalid_shard_id(self):
        p = self._write_cfg(
            {
                "cluster_endpoints": ["10.0.0.1"],
                "scrape_duration": 10,
                "port": 9095,
                "scan_new_nodes_interval": 10,
                "jmx_items": {"m1": {"objectName": "o", "attribute": "a"}},
                "shard_total": 2,
                "shard_id": 2,
            }
        )
        try:
            with self.assertRaises(ConfigError):
                Config.load(p)
        finally:
            os.unlink(p)


class MonitorLogicTests(unittest.TestCase):
    def setUp(self):
        self.cfg = Config(
            jmx_items={"m1": {"objectName": "o", "attribute": "a"}},
            cluster_endpoints=["10.0.0.1"],
            scrape_duration=30,
            port=9095,
            scan_new_nodes_interval=120,
            jmx_port=7199,
            max_workers=2,
            disable_internal_metrics=True,
            jmx_connect_timeout=5,
            shard_total=1,
            shard_id=0,
            scrape_batch_size=0,
        )

    def test_shard_filtering_partitions_nodes(self):
        mm = MetricsManager()
        health = HealthCheck()
        mon = JMXMonitor(self.cfg, mm, internal_metrics=jm.NoOpMetrics(), health_check=health)

        mon.config.shard_total = 2
        mon.config.shard_id = 0
        ips = ["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"]
        s0 = {ip for ip in ips if mon._node_in_my_shard(ip)}

        mon.config.shard_id = 1
        s1 = {ip for ip in ips if mon._node_in_my_shard(ip)}

        self.assertEqual(s0.union(s1), set(ips))
        self.assertTrue(s0.isdisjoint(s1))

    def test_batch_selection_rotates(self):
        cfg = self.cfg
        cfg.scrape_batch_size = 2

        mm = MetricsManager()
        health = HealthCheck()
        mon = JMXMonitor(cfg, mm, internal_metrics=jm.NoOpMetrics(), health_check=health)

        nodes = ["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"]
        a, skipped_a = mon._select_batch(nodes)
        b, skipped_b = mon._select_batch(nodes)

        self.assertEqual(len(a), 2)
        self.assertEqual(len(b), 2)
        self.assertEqual(skipped_a, 2)
        self.assertEqual(skipped_b, 2)
        self.assertNotEqual(set(a), set(b))

    def test_collect_uses_prebuilt_queries(self):
        mm = mock.Mock()
        health = HealthCheck()
        mon = JMXMonitor(self.cfg, mm, internal_metrics=jm.NoOpMetrics(), health_check=health)

        fake_metric = types.SimpleNamespace(metric_name="m1", value=5)
        fake_conn = mock.MagicMock()
        fake_conn.query.return_value = [fake_metric]

        with mock.patch.object(jm.JMXConnectionCache, "get", return_value=fake_conn):
            mon._collect_from_single_node("10.0.0.8")

        args, _ = fake_conn.query.call_args
        self.assertIs(args[0], mon._queries)  # identity check
        mm.set_metric_value.assert_called_with("m1", "10.0.0.8", 5.0)

    def test_scrape_cycle_skips_cooldown_nodes(self):
        cfg = self.cfg
        cfg.max_workers = 1

        mm = MetricsManager()
        health = HealthCheck()
        mon = JMXMonitor(cfg, mm, internal_metrics=jm.NoOpMetrics(), health_check=health)

        with mon._lock:
            mon.cluster_node_list = ["10.0.0.1", "10.0.0.2"]
            mon._node_skip_until["10.0.0.2"] = time.time() + 999

        with mock.patch.object(mon, "_collect_from_single_node", return_value=None) as c:
            mon._run_scrape_cycle()
            c.assert_called_once_with("10.0.0.1")


if __name__ == "__main__":
    unittest.main()

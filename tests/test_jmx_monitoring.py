import os
import tempfile
import types
import unittest
from unittest import mock

import yaml

import jmx_monitoring
from jmx_monitoring import Config, ConfigError, HealthCheck, JMXMonitor, MetricsManager


class ConfigLoadTests(unittest.TestCase):
    def setUp(self):
        self.original_env = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.original_env)

    def test_load_configuration_with_env_overrides(self):
        config_data = {
            'cluster_endpoint': '10.0.0.1',
            'scrape_duration': 30,
            'port': 9000,
            'scan_new_nodes_interval': 45,
            'jmx_port': 7100,
            'max_workers': 5,
            'jmx_items': {
                'pending_tasks': {
                    'objectName': 'org.apache.cassandra.metrics:type=ThreadPools',
                    'attribute': 'PendingTasks'
                }
            }
        }

        with tempfile.NamedTemporaryFile('w', delete=False) as tmp:
            yaml.safe_dump(config_data, tmp)
            tmp_path = tmp.name

        try:
            os.environ['CASSANDRA_CLUSTER_ENDPOINT'] = '10.0.0.2'
            os.environ['JMX_SCRAPE_DURATION'] = '120'
            os.environ['JMX_EXPORTER_PORT'] = '9100'
            os.environ['NODE_SCAN_INTERVAL'] = '60'
            os.environ['JMX_PORT'] = '7200'
            os.environ['MAX_WORKERS'] = '10'

            config = Config.load(tmp_path)

            self.assertEqual(config.cluster_endpoint, '10.0.0.2')
            self.assertEqual(config.scrape_duration, 120)
            self.assertEqual(config.port, 9100)
            self.assertEqual(config.scan_new_nodes_interval, 60)
            self.assertEqual(config.jmx_port, 7200)
            self.assertEqual(config.max_workers, 10)
            self.assertEqual(config.jmx_items, config_data['jmx_items'])
        finally:
            os.unlink(tmp_path)

    def test_load_configuration_missing_cluster_endpoint_raises(self):
        config_data = {
            'scrape_duration': 30,
            'port': 9000,
            'scan_new_nodes_interval': 45,
            'jmx_items': {
                'pending_tasks': {
                    'objectName': 'org.apache.cassandra.metrics:type=ThreadPools',
                    'attribute': 'PendingTasks'
                }
            }
        }

        with tempfile.NamedTemporaryFile('w', delete=False) as tmp:
            yaml.safe_dump(config_data, tmp)
            tmp_path = tmp.name

        try:
            with self.assertRaises(ConfigError):
                Config.load(tmp_path)
        finally:
            os.unlink(tmp_path)


class HealthCheckTests(unittest.TestCase):
    def test_health_check_app_responses(self):
        health = HealthCheck()

        # Liveness should always be OK
        status, body = self._invoke_app(health, '/health/live')
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, b'OK')

        # Readiness should be 503 until ready
        status, body = self._invoke_app(health, '/health/ready')
        self.assertEqual(status, '503 Service Unavailable')
        self.assertEqual(body, b'Not Ready')

        health.set_ready(True)

        status, body = self._invoke_app(health, '/health/ready')
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, b'Ready')

        status, body = self._invoke_app(health, '/unknown')
        self.assertEqual(status, '404 Not Found')
        self.assertEqual(body, b'Not Found')

    @staticmethod
    def _invoke_app(health, path):
        environ = {'PATH_INFO': path}
        captured = {}

        def start_response(status, headers):
            captured['status'] = status
            captured['headers'] = headers

        result = b''.join(health.app(environ, start_response))
        return captured['status'], result


class JMXMonitorTests(unittest.TestCase):
    def setUp(self):
        self.config = Config(
            jmx_items={
                'pending_tasks': {
                    'objectName': 'org.apache.cassandra.metrics:type=ThreadPools',
                    'attribute': 'PendingTasks'
                }
            },
            cluster_endpoint='10.0.0.1',
            scrape_duration=60,
            port=9095,
            scan_new_nodes_interval=120,
            jmx_port=7199,
            max_workers=5
        )
        self.metrics_manager = MetricsManager()
        self.metrics_manager.set_cluster_name('test-cluster')
        self.internal_metrics = mock.Mock()
        self.health_check = mock.Mock()
        self.monitor = JMXMonitor(self.config, self.metrics_manager, self.internal_metrics, self.health_check)

    def test_discover_nodes_returns_only_up_nodes(self):
        fake_metric = types.SimpleNamespace(value='/10.0.0.2=UP,/10.0.0.3=DOWN,/10.0.0.4=UP')
        fake_conn = mock.MagicMock()
        fake_conn.query.return_value = [fake_metric]

        with mock.patch.object(jmx_monitoring.jmxquery, 'JMXConnection', return_value=fake_conn):
            nodes = self.monitor._discover_nodes()

        self.assertEqual(nodes, ['10.0.0.2', '10.0.0.4'])

    def test_collect_from_single_node_sets_metric_values(self):
        fake_metric = types.SimpleNamespace(metric_name='pending_tasks', value=5)
        fake_conn = mock.MagicMock()
        fake_conn.query.return_value = [fake_metric]

        with mock.patch.object(jmx_monitoring.jmxquery, 'JMXConnection', return_value=fake_conn):
            with mock.patch.object(self.metrics_manager, 'set_metric_value') as set_metric_value:
                self.monitor._collect_from_single_node('10.0.0.2')

        set_metric_value.assert_called_once_with('pending_tasks', '10.0.0.2', 5.0)


if __name__ == '__main__':
    unittest.main()

import os
import tempfile
import types
import unittest
import time
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
            'cluster_endpoints': ['10.0.0.1'],
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
            os.environ['CASSANDRA_CLUSTER_ENDPOINTS'] = '10.0.0.2, 10.0.0.3'
            os.environ['JMX_SCRAPE_DURATION'] = '120'
            os.environ['JMX_EXPORTER_PORT'] = '9100'
            os.environ['NODE_SCAN_INTERVAL'] = '60'
            os.environ['JMX_PORT'] = '7200'
            os.environ['MAX_WORKERS'] = '10'

            config = Config.load(tmp_path)

            self.assertEqual(config.cluster_endpoints, ['10.0.0.2', '10.0.0.3'])
            self.assertEqual(config.scrape_duration, 120)
            self.assertEqual(config.port, 9100)
            self.assertEqual(config.scan_new_nodes_interval, 60)
            self.assertEqual(config.jmx_port, 7200)
            self.assertEqual(config.max_workers, 10)
            self.assertEqual(config.jmx_items, config_data['jmx_items'])
        finally:
            os.unlink(tmp_path)

    def test_load_configuration_supports_legacy_single_endpoint_env(self):
        config_data = {
            'cluster_endpoints': ['10.0.0.1'],
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

            config = Config.load(tmp_path)

            self.assertEqual(config.cluster_endpoints, ['10.0.0.2'])
        finally:
            os.unlink(tmp_path)

    def test_load_configuration_missing_cluster_endpoints_raises(self):
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
            cluster_endpoints=['10.0.0.1', '10.0.0.5'],
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

        with mock.patch.object(jmx_monitoring, 'create_jmx_connection', return_value=fake_conn):
            nodes = self.monitor._discover_nodes()

        self.assertEqual(nodes, ['10.0.0.2', '10.0.0.4'])

    def test_discover_nodes_falls_back_to_second_endpoint(self):
        fake_metric = types.SimpleNamespace(value='/10.0.0.6=UP')
        successful_conn = mock.MagicMock()
        successful_conn.query.return_value = [fake_metric]

        def fake_create_connection(url):
            if '10.0.0.1' in url:
                raise Exception('host unreachable')
            return successful_conn

        with mock.patch.object(jmx_monitoring, 'create_jmx_connection', side_effect=fake_create_connection):
            nodes = self.monitor._discover_nodes()

        self.assertEqual(nodes, ['10.0.0.6'])

    def test_collect_from_single_node_sets_metric_values(self):
        fake_metric = types.SimpleNamespace(metric_name='pending_tasks', value=5)
        fake_conn = mock.MagicMock()
        fake_conn.query.return_value = [fake_metric]

        with mock.patch.object(jmx_monitoring, 'create_jmx_connection', return_value=fake_conn):
            with mock.patch.object(self.metrics_manager, 'set_metric_value') as set_metric_value:
                self.monitor._collect_from_single_node('10.0.0.2')

        set_metric_value.assert_called_once_with('pending_tasks', '10.0.0.2', 5.0)

    def test_discover_nodes_logs_local_jmx_hint_once(self):
        error_message = (
            "Error calling JMX: { \"error\": \"connection-error\", \"message\":\"Connection refused to host: 127.0.0.1; "
            "nested exception is: java.net.ConnectException: Connection refused\"}"
        )

        with mock.patch.object(jmx_monitoring, 'create_jmx_connection', side_effect=Exception(error_message)):
            with self.assertLogs(jmx_monitoring.logger, level='WARNING') as captured_logs:
                self.monitor._discover_nodes()

        matching_logs = [entry for entry in captured_logs.output if 'LOCAL_JMX' in entry]
        self.assertEqual(len(matching_logs), 1)


class JMXMonitorTimeoutTests(unittest.TestCase):
    def setUp(self):
        self.config = Config(
            jmx_items={'m1': {'objectName': 'obj', 'attribute': 'attr'}},
            cluster_endpoints=['10.0.0.1'],
            scrape_duration=1,  # Short duration for testing
            port=9095,
            scan_new_nodes_interval=120,
            jmx_port=7199,
            max_workers=5
        )
        self.metrics_manager = mock.Mock()
        self.internal_metrics = mock.Mock()
        
        # Setup mock metrics with labels
        self.mock_node_scrape_errors = mock.Mock()
        self.internal_metrics.node_scrape_errors_total.labels.return_value = self.mock_node_scrape_errors
        
        self.mock_node_errors = mock.Mock()
        self.internal_metrics.node_errors_total.labels.return_value = self.mock_node_errors
        
        self.mock_node_timeouts = mock.Mock()
        self.internal_metrics.node_timeouts_total.labels.return_value = self.mock_node_timeouts
        
        self.mock_node_query_duration = mock.Mock()
        self.internal_metrics.node_query_duration_seconds.labels.return_value = self.mock_node_query_duration
        
        self.internal_metrics.scrapes_in_progress = mock.Mock()
        self.internal_metrics.scrape_duration_seconds = mock.Mock()
        self.internal_metrics.scrapes_total = mock.Mock()
        
        self.health_check = mock.Mock()
        self.monitor = JMXMonitor(self.config, self.metrics_manager, self.internal_metrics, self.health_check)
        self.monitor.cluster_node_list = ['10.0.0.1', '10.0.0.2']

    def test_scrape_cycle_finishes_when_node_hangs(self):
        """Test that a hanging node scrape does not block the entire cycle."""
        def mock_collect(ip):
            if ip == '10.0.0.2':
                time.sleep(5)  # Hang longer than the budget
            return

        with mock.patch.object(self.monitor, '_collect_from_single_node', side_effect=mock_collect):
            start_time = time.time()
            self.monitor._run_scrape_cycle()
            duration = time.time() - start_time

        # Budget is scrape_duration (1) - 0.5 = 0.5s
        # The cycle should finish shortly after 0.5s, not wait 5s
        self.assertLess(duration, 2.0)
        self.assertGreaterEqual(duration, 0.4)

        # Verify timeout was recorded for the hanging node
        self.internal_metrics.node_errors_total.labels.assert_any_call(ip='10.0.0.2', error_type='timeout')
        self.mock_node_errors.inc.assert_called()
        self.internal_metrics.node_timeouts_total.labels.assert_any_call(ip='10.0.0.2')
        self.mock_node_timeouts.inc.assert_called()

    def test_other_nodes_still_succeed_when_one_hangs(self):
        """Test that other nodes still complete their scrapes even if one hangs."""
        success_nodes = []
        def mock_collect(ip):
            if ip == '10.0.0.2':
                time.sleep(5)
            else:
                success_nodes.append(ip)
            return

        with mock.patch.object(self.monitor, '_collect_from_single_node', side_effect=mock_collect):
            self.monitor._run_scrape_cycle()

        self.assertIn('10.0.0.1', success_nodes)
        self.assertNotIn('10.0.0.2', success_nodes)

    def test_scrapes_in_progress_is_reset(self):
        """Test that scrapes_in_progress is reset even if there are timeouts."""
        def mock_collect(ip):
            time.sleep(2)
            return

        with mock.patch.object(self.monitor, '_collect_from_single_node', side_effect=mock_collect):
            self.monitor._run_scrape_cycle()

        # Should be set to 0 at the end
        self.internal_metrics.scrapes_in_progress.set.assert_called_with(0)


if __name__ == '__main__':
    unittest.main()

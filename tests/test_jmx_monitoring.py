import os
import tempfile
import types
import threading
import unittest
import time
from unittest import mock

import yaml

import jmx_monitoring
from jmx_monitoring import Config, ConfigError, HealthCheck, JMXMonitor, MetricsManager, JMXConnectionCache


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
        
        self.mock_node_skipped = mock.Mock()
        self.internal_metrics.node_skipped_total.labels.return_value = self.mock_node_skipped

        self.mock_cancel_failed = mock.Mock()
        self.internal_metrics.cancel_failed_total.labels.return_value = self.mock_cancel_failed

        self.internal_metrics.nodes_scheduled = mock.Mock()
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

    def test_cancel_failure_handling(self):
        """Test that cancel failure is recorded when a future cannot be cancelled."""
        # We need to mock the executor to return a future that fails to cancel
        mock_future = mock.Mock()
        mock_future.cancel.return_value = False  # Cancel failed
        mock_future.result.side_effect = Exception("Should not be called")

        # Create a mock executor
        mock_executor = mock.Mock()
        mock_executor.submit.return_value = mock_future

        # Replace the monitor's executor
        self.monitor.executor = mock_executor

        # Mock wait to return the future as not_done
        with mock.patch('concurrent.futures.wait', return_value=([], [mock_future])):
            with mock.patch.object(self.monitor._jmx_cache, 'invalidate') as mock_invalidate:
                self.monitor._run_scrape_cycle()

        # Verify cancel was called
        mock_future.cancel.assert_called()
        # Verify cancel failure metric was incremented
        self.internal_metrics.cancel_failed_total.labels.assert_called()
        self.mock_cancel_failed.inc.assert_called()
        # Verify cache invalidate was called for the timed out node
        mock_invalidate.assert_called_once_with("service:jmx:rmi:///jndi/rmi://10.0.0.2:7199/jmxrmi")
        # Verify severe timeout: consecutive timeouts set to threshold and skip_until set
        self.assertEqual(self.monitor._node_consecutive_timeouts['10.0.0.2'], 3)  # threshold
        self.assertIn('10.0.0.2', self.monitor._node_skip_until)
        self.assertGreater(self.monitor._node_skip_until['10.0.0.2'], time.time())

    def test_timeout_invalidates_cache(self):
        """Test that cache is invalidated for timed out nodes."""
        mock_future = mock.Mock()
        mock_future.cancel.return_value = True  # Cancel succeeds

        mock_executor = mock.Mock()
        mock_executor.submit.return_value = mock_future

        self.monitor.executor = mock_executor

        with mock.patch('concurrent.futures.wait', return_value=([], [mock_future])):
            with mock.patch.object(self.monitor._jmx_cache, 'invalidate') as mock_invalidate:
                self.monitor._run_scrape_cycle()

        # Verify cache invalidate was called for the timed out node
        mock_invalidate.assert_called_once_with("service:jmx:rmi:///jndi/rmi://10.0.0.2:7199/jmxrmi")
        # Since cancel succeeded, consecutive timeouts incremented normally
        self.assertEqual(self.monitor._node_consecutive_timeouts['10.0.0.2'], 1)

    def test_skip_mechanism(self):
        """Test that a node is skipped after N consecutive timeouts."""
        self.monitor._timeout_threshold = 2
        self.monitor._skip_cooldown = 60
        
        # 1st timeout
        m_future1 = mock.Mock()
        m_future2 = mock.Mock()
        m_future1.cancel.return_value = True
        m_future2.cancel.return_value = True
        
        # We need to mock submit to return different futures for different nodes
        def mock_submit(fn, ip):
            if ip == '10.0.0.1': return m_future1
            return m_future2

        # We need to mock wait to return our futures in not_done
        with mock.patch('concurrent.futures.wait', return_value=([], [m_future1, m_future2])):
            with mock.patch.object(self.monitor.executor, 'submit', side_effect=mock_submit):
                self.monitor._run_scrape_cycle()
        
        self.assertEqual(self.monitor._node_consecutive_timeouts['10.0.0.1'], 1)
        self.assertEqual(self.monitor._node_consecutive_timeouts['10.0.0.2'], 1)
        
        # 2nd timeout -> should trigger skip
        with mock.patch('concurrent.futures.wait', return_value=([], [m_future1, m_future2])):
            with mock.patch.object(self.monitor.executor, 'submit', side_effect=mock_submit):
                self.monitor._run_scrape_cycle()

        self.assertEqual(self.monitor._node_consecutive_timeouts['10.0.0.1'], 2)
        self.assertIn('10.0.0.1', self.monitor._node_skip_until)
        self.assertGreater(self.monitor._node_skip_until['10.0.0.1'], time.time())

        # 3rd cycle -> should skip
        with mock.patch.object(self.monitor.executor, 'submit') as mock_submit_call:
            self.monitor._run_scrape_cycle()
            # Should not have been called because nodes are skipped
            mock_submit_call.assert_not_called()
            
        # Verify skip metric
        self.internal_metrics.node_skipped_total.labels.assert_any_call(ip='10.0.0.1', reason='timeout')
        self.mock_node_skipped.inc.assert_called()

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

        # Should be set to 1 at start and 0 at the end
        self.internal_metrics.scrapes_in_progress.set.assert_any_call(1)
        self.internal_metrics.scrapes_in_progress.set.assert_called_with(0)

    def test_nodes_scheduled_metric(self):
        """Test that nodes_scheduled metric is set correctly."""
        self.monitor.cluster_node_list = ['10.0.0.1', '10.0.0.2', '10.0.0.3']
        
        with mock.patch.object(self.monitor, '_collect_from_single_node'):
            self.monitor._run_scrape_cycle()
            
        self.internal_metrics.nodes_scheduled.set.assert_called_with(3)


class JMXMonitorShutdownTests(unittest.TestCase):
    """Tests for shutdown responsiveness of the node discovery loop."""
    
    def setUp(self):
        self.config = Config(
            jmx_items={'m1': {'objectName': 'obj', 'attribute': 'attr'}},
            cluster_endpoints=['10.0.0.1'],
            scrape_duration=1,
            port=9095,
            scan_new_nodes_interval=60,  # Large interval for testing
            jmx_port=7199,
            max_workers=5
        )
        self.metrics_manager = mock.Mock()
        self.internal_metrics = mock.Mock()
        self.health_check = mock.Mock()
        self.monitor = JMXMonitor(self.config, self.metrics_manager, self.internal_metrics, self.health_check)

    def test_discovery_loop_exits_quickly_on_shutdown(self):
        """Test that the discovery thread terminates quickly when shutdown is requested."""
        # Set a very large scan interval
        self.monitor.config.scan_new_nodes_interval = 60
        
        # Mock _discover_nodes to return empty list quickly
        with mock.patch.object(self.monitor, '_discover_nodes', return_value=[]):
            # Start the discovery thread manually
            discovery_thread = threading.Thread(target=self.monitor._update_node_list, daemon=True)
            discovery_thread.start()
            
            # Let it run for a bit to enter the wait
            time.sleep(0.1)
            
            # Trigger shutdown
            start_time = time.time()
            self.monitor._shutdown.set()
            
            # Join with a reasonable timeout
            discovery_thread.join(timeout=0.5)
            shutdown_duration = time.time() - start_time
            
            # Verify the thread exited quickly (much less than the 60 second interval)
            self.assertFalse(discovery_thread.is_alive(), "Discovery thread should have exited")
            self.assertLess(shutdown_duration, 0.5,
                          f"Shutdown took {shutdown_duration}s, expected < 0.5s")

    def test_no_time_sleep_in_discovery_loop(self):
        """Test that time.sleep is not used in the discovery loop (should use Event.wait instead)."""
        # Monkeypatch time.sleep to raise an exception if called
        original_sleep = time.sleep
        sleep_called = []
        
        def raise_on_sleep(duration):
            sleep_called.append(duration)
            raise AssertionError(f"time.sleep({duration}) was called, but Event.wait should be used instead")
        
        # Mock _discover_nodes to return immediately
        with mock.patch.object(self.monitor, '_discover_nodes', return_value=['10.0.0.1']):
            # Patch time.sleep
            with mock.patch('time.sleep', side_effect=raise_on_sleep):
                # Start the discovery thread
                discovery_thread = threading.Thread(target=self.monitor._update_node_list, daemon=True)
                discovery_thread.start()
                
                # Let it run one iteration
                time_module = __import__('time')
                original_sleep(0.1)
                
                # Trigger shutdown to exit the loop
                self.monitor._shutdown.set()
                discovery_thread.join(timeout=1.0)
        
        # If we get here, time.sleep was not called in _update_node_list
        self.assertEqual(sleep_called, [], "time.sleep should not have been called")

    def test_shutdown_joins_discovery_thread(self):
        """Test that shutdown waits for the discovery thread to exit."""
        # Mock _discover_nodes to simulate work
        discover_call_count = []
        
        def mock_discover():
            discover_call_count.append(1)
            return ['10.0.0.1']
        
        with mock.patch.object(self.monitor, '_discover_nodes', side_effect=mock_discover):
            # Start the monitor's discovery thread manually
            self.monitor._discovery_thread = threading.Thread(
                target=self.monitor._update_node_list,
                daemon=True,
                name='node-discovery'
            )
            self.monitor._discovery_thread.start()
            
            # Let it run for a bit
            time.sleep(0.1)
            
            # Verify thread is alive
            self.assertTrue(self.monitor._discovery_thread.is_alive())
            
            # Call shutdown
            self.monitor.shutdown()
            
            # Verify the thread is no longer alive after shutdown
            self.assertFalse(self.monitor._discovery_thread.is_alive(),
                           "Discovery thread should have been joined and exited")

    def test_discovery_loop_handles_exceptions_during_shutdown(self):
        """Test that exceptions during shutdown don't spam logs."""
        exception_raised = False
        
        def mock_discover():
            nonlocal exception_raised
            if not exception_raised:
                exception_raised = True
                raise Exception("Discovery error")
            return []
        
        with mock.patch.object(self.monitor, '_discover_nodes', side_effect=mock_discover):
            # Start discovery thread
            discovery_thread = threading.Thread(target=self.monitor._update_node_list, daemon=True)
            discovery_thread.start()
            
            # Let it process the exception
            time.sleep(0.1)
            
            # Now trigger shutdown
            self.monitor._shutdown.set()
            
            # Second iteration should not log because shutdown is set
            # We verify the thread exits cleanly
            discovery_thread.join(timeout=0.5)
            self.assertFalse(discovery_thread.is_alive())


class JMXConnectionCacheTests(unittest.TestCase):
    def setUp(self):
        self.original_env = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.original_env)

    @mock.patch('jmx_monitoring.create_jmx_connection')
    def test_cache_reuse_within_ttl(self, mock_create):
        """Test A: Reuse within TTL"""
        mock_conn1 = mock.Mock()
        mock_conn2 = mock.Mock()
        mock_create.side_effect = [mock_conn1, mock_conn2]

        cache = JMXConnectionCache()
        url = "service:jmx:rmi:///jndi/rmi://10.0.0.1:7199/jmxrmi"

        # First get
        conn1 = cache.get(url)
        self.assertEqual(conn1, mock_conn1)
        self.assertEqual(mock_create.call_count, 1)

        # Second get within TTL
        conn2 = cache.get(url)
        self.assertEqual(conn2, mock_conn1)  # Same object
        self.assertEqual(mock_create.call_count, 1)  # Not called again

    @mock.patch('jmx_monitoring.create_jmx_connection')
    @mock.patch('jmx_monitoring.time.time')
    def test_cache_recreate_after_ttl_expiry(self, mock_time, mock_create):
        """Test B: Recreate after TTL expiry"""
        mock_time.side_effect = [100.0, 250.0, 371.0, 371.0]  # store, check (expired), store new
        mock_conn1 = mock.Mock()
        mock_conn2 = mock.Mock()
        mock_create.side_effect = [mock_conn1, mock_conn2]

        os.environ['JMX_CONN_CACHE_TTL_SECONDS'] = '120'
        cache = JMXConnectionCache()
        url = "service:jmx:rmi:///jndi/rmi://10.0.0.1:7199/jmxrmi"

        # First get
        conn1 = cache.get(url)
        self.assertEqual(conn1, mock_conn1)

        # Second get after TTL (371 - 250 = 121 > 120)
        conn2 = cache.get(url)
        self.assertEqual(conn2, mock_conn2)
        self.assertEqual(mock_create.call_count, 2)
        mock_conn1.close.assert_called_once()

    @mock.patch('jmx_monitoring.create_jmx_connection')
    def test_cache_eviction_on_max_size(self, mock_create):
        """Test C: Eviction closes connections"""
        mock_conns = [mock.Mock() for _ in range(3)]
        mock_create.side_effect = mock_conns

        os.environ['JMX_CONN_CACHE_MAX_SIZE'] = '2'
        cache = JMXConnectionCache()

        urls = [
            "service:jmx:rmi:///jndi/rmi://10.0.0.1:7199/jmxrmi",
            "service:jmx:rmi:///jndi/rmi://10.0.0.2:7199/jmxrmi",
            "service:jmx:rmi:///jndi/rmi://10.0.0.3:7199/jmxrmi"
        ]

        # Get first two
        cache.get(urls[0])
        cache.get(urls[1])

        # Get third, should evict first
        cache.get(urls[2])

        self.assertEqual(mock_create.call_count, 3)
        mock_conns[0].close.assert_called_once()  # First evicted
        mock_conns[1].close.assert_not_called()
        mock_conns[2].close.assert_not_called()

    @mock.patch('jmx_monitoring.create_jmx_connection')
    def test_shutdown_closes_all(self, mock_create):
        """Test D: Shutdown closes all"""
        mock_conns = [mock.Mock() for _ in range(2)]
        mock_create.side_effect = mock_conns

        cache = JMXConnectionCache()
        urls = [
            "service:jmx:rmi:///jndi/rmi://10.0.0.1:7199/jmxrmi",
            "service:jmx:rmi:///jndi/rmi://10.0.0.2:7199/jmxrmi"
        ]

        cache.get(urls[0])
        cache.get(urls[1])

        cache.close_all()

        mock_conns[0].close.assert_called_once()
        mock_conns[1].close.assert_called_once()

    @mock.patch('jmx_monitoring.create_jmx_connection')
    def test_lock_not_held_during_connection_creation(self, mock_create):
        """Test A: Lock is not held during connection creation"""
        import threading
        import time as time_module

        # Event to control when create_jmx_connection returns for url1
        create_event = threading.Event()
        mock_conn1 = mock.Mock()
        mock_conn2 = mock.Mock()
        def create_side_effect(url, **kwargs):
            if '10.0.0.1' in url:
                create_event.wait()  # Block for url1
                return mock_conn1
            else:
                return mock_conn2  # Immediate for url2

        mock_create.side_effect = create_side_effect

        cache = JMXConnectionCache()
        url1 = "service:jmx:rmi:///jndi/rmi://10.0.0.1:7199/jmxrmi"
        url2 = "service:jmx:rmi:///jndi/rmi://10.0.0.2:7199/jmxrmi"

        # Start first get in a thread, it will block in create_jmx_connection
        result = [None]
        def thread1():
            result[0] = cache.get(url1)
        t1 = threading.Thread(target=thread1)
        t1.start()

        # Wait a bit for it to start creating
        time_module.sleep(0.1)

        # Now try to get another URL; should not block on lock
        start_time = time_module.time()
        conn2 = cache.get(url2)  # This should proceed quickly since lock is not held
        elapsed = time_module.time() - start_time
        self.assertLess(elapsed, 0.5)  # Should not take long
        self.assertEqual(conn2, mock_conn2)

        # Now release the first create
        create_event.set()
        t1.join(timeout=1.0)
        self.assertEqual(result[0], mock_conn1)

    @mock.patch('jmx_monitoring.create_jmx_connection')
    def test_close_all_does_not_hold_lock_while_closing(self, mock_create):
        """Test B: close_all does not hold lock while closing"""
        import threading
        import time as time_module

        close_event = threading.Event()
        mock_conns = [mock.Mock() for _ in range(2)]
        # Make close block until event is set
        for conn in mock_conns:
            conn.close.side_effect = lambda: close_event.wait()

        mock_create.side_effect = mock_conns

        cache = JMXConnectionCache()
        urls = [
            "service:jmx:rmi:///jndi/rmi://10.0.0.1:7199/jmxrmi",
            "service:jmx:rmi:///jndi/rmi://10.0.0.2:7199/jmxrmi"
        ]

        cache.get(urls[0])
        cache.get(urls[1])

        # Start close_all in thread
        def close_thread():
            cache.close_all()
        t = threading.Thread(target=close_thread)
        t.start()

        # Wait a bit, then try to get something; should not block
        time_module.sleep(0.1)
        start_time = time_module.time()
        # This should proceed since lock is not held during close
        # But since cache is cleared, it will try to create new
        mock_create.side_effect = [mock.Mock()]  # For the new get
        conn = cache.get(urls[0])
        elapsed = time_module.time() - start_time
        self.assertLess(elapsed, 0.5)

        # Release close
        close_event.set()
        t.join(timeout=1.0)

    def test_invalidate_removes_and_closes_connection(self):
        """Test invalidate removes and closes cached connection"""
        cache = JMXConnectionCache()
        url = "service:jmx:rmi:///jndi/rmi://10.0.0.1:7199/jmxrmi"
        mock_conn = mock.Mock()

        # Manually add to cache
        with cache._lock:
            cache._cache[url] = (mock_conn, time.time())

        cache.invalidate(url)

        # Should be removed
        with cache._lock:
            self.assertNotIn(url, cache._cache)
        # Should be closed
        mock_conn.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()

#!/usr/bin/python3
import logging
import socket
import time
import jmxquery
import threading
from prometheus_client import Counter, Gauge, Histogram, Info
import re
import os
import yaml
import sys
import signal
import argparse
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import json
from wsgiref.simple_server import make_server
import concurrent.futures
import inspect

VERSION = "1.5.1"

_JMX_CONNECTION_SUPPORTS_TIMEOUT = 'timeout' in inspect.signature(jmxquery.JMXConnection).parameters


def create_jmx_connection(jmx_url: str, timeout: int = 10) -> jmxquery.JMXConnection:
    """Create a JMXConnection while gracefully handling timeout support."""
    if _JMX_CONNECTION_SUPPORTS_TIMEOUT:
        return jmxquery.JMXConnection(jmx_url, timeout=timeout)
    logger.debug("jmxquery.JMXConnection does not support timeout parameter; creating connection without it.")
    return jmxquery.JMXConnection(jmx_url)

# Configure logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Custom Exception Classes ---
class JMXExporterError(Exception):
    """Base exception for JMX Exporter"""
    pass

class ConnectionError(JMXExporterError):
    """Raised when JMX connection fails"""
    pass

class ConfigError(JMXExporterError):
    """Raised when configuration is invalid"""
    pass

# --- Internal Metrics for the Exporter Itself ---
class InternalMetrics:
    def __init__(self):
        self.info = Info('jmx_exporter', 'Information about the JMX Exporter')
        self.scrapes_total = Counter(
            'jmx_exporter_scrapes_total',
            'Total number of scrape cycles completed.'
        )
        self.scrape_duration_seconds = Gauge(
            'jmx_exporter_scrape_duration_seconds',
            'Duration of the last scrape cycle in seconds.'
        )
        self.scrapes_in_progress = Gauge(
            'jmx_exporter_scrapes_in_progress',
            'Number of scrape cycles currently running.'
        )
        self.nodes_discovered = Gauge(
            'jmx_exporter_nodes_discovered',
            'The current number of Cassandra nodes being monitored.'
        )
        self.discovery_duration_seconds = Histogram(
            'jmx_exporter_discovery_duration_seconds',
            'Time spent discovering nodes in seconds.'
        )
        self.discovery_errors_total = Counter(
            'jmx_exporter_discovery_errors_total',
            'Total number of errors encountered during node discovery.'
        )
        self.node_scrape_errors_total = Counter(
            'jmx_exporter_node_scrape_errors_total',
            'Total number of scrape errors per node.',
            ['ip']
        )
        self.node_errors_total = Counter(
            'jmx_exporter_node_errors_total',
            'Categorized scrape errors per node.',
            ['ip', 'error_type']
        )
        self.node_timeouts_total = Counter(
            'jmx_exporter_node_timeouts_total',
            'Total number of timeout errors per node.',
            ['ip']
        )
        self.node_connect_duration_seconds = Histogram(
            'jmx_exporter_node_connect_duration_seconds',
            'Time to establish a JMX connection per node.',
            ['ip']
        )
        self.node_query_duration_seconds = Histogram(
            'jmx_exporter_node_query_duration_seconds',
            'Time to execute the JMX query per node.',
            ['ip']
        )
        self.node_samples_emitted = Gauge(
            'jmx_exporter_node_samples_emitted',
            'Number of samples emitted for a node during the last scrape.',
            ['ip']
        )
        self.node_last_success_timestamp_seconds = Gauge(
            'jmx_exporter_node_last_success_timestamp_seconds',
            'Unix timestamp of the last successful scrape per node.',
            ['ip']
        )
        self.node_skipped_total = Counter(
            'jmx_exporter_nodes_skipped_total',
            'Total number of times a node was skipped due to repeated timeouts.',
            ['ip', 'reason']
        )
        self.cancel_failed_total = Counter(
            'jmx_exporter_cancel_failed_total',
            'Total number of times a future cancel failed (task already running).',
            ['ip']
        )
        self.nodes_scheduled = Gauge(
            'jmx_exporter_nodes_scheduled',
            'Number of nodes scheduled for scraping in the current cycle.',
        )

class NoOpMetric:
    """A mock metric class that performs no operations."""
    def inc(self, *args, **kwargs): pass
    def set(self, *args, **kwargs): pass
    def observe(self, *args, **kwargs): pass
    def labels(self, *args, **kwargs): return self
    def info(self, *args, **kwargs): pass

class NoOpMetrics:
    """A placeholder for InternalMetrics that does nothing."""
    def __init__(self):
        noop = NoOpMetric()
        self.info = noop
        self.scrapes_total = noop
        self.scrape_duration_seconds = noop
        self.scrapes_in_progress = noop
        self.nodes_discovered = noop
        self.discovery_duration_seconds = noop
        self.discovery_errors_total = noop
        self.node_scrape_errors_total = noop
        self.node_errors_total = noop
        self.node_timeouts_total = noop
        self.node_connect_duration_seconds = noop
        self.node_query_duration_seconds = noop
        self.node_samples_emitted = noop
        self.node_last_success_timestamp_seconds = noop
        self.node_skipped_total = noop
        self.cancel_failed_total = noop
        self.nodes_scheduled = noop


# --- Configuration Management ---
@dataclass
class Config:
    """Configuration class supporting both YAML and environment variables"""
    jmx_items: Dict[str, Any]
    cluster_endpoints: List[str]
    scrape_duration: int
    port: int
    scan_new_nodes_interval: int
    jmx_port: int = 7199
    max_workers: int = 20
    disable_internal_metrics: bool = False

    @classmethod
    def load(cls, config_path: str = None) -> 'Config':
        """
        Load configuration from environment variables and YAML file, with env vars taking precedence.
        """
        try:
            final_path = config_path or os.getenv('CONFIG_PATH', '/etc/jmx-exporter/config.yml')
            logger.info(f"Loading configuration from: {final_path}")

            with open(final_path) as f:
                config_data = yaml.safe_load(f) or {}

            env_cluster_endpoints = os.getenv('CASSANDRA_CLUSTER_ENDPOINTS')
            env_cluster_endpoint = os.getenv('CASSANDRA_CLUSTER_ENDPOINT')

            if env_cluster_endpoints:
                cluster_endpoints = [ip.strip() for ip in env_cluster_endpoints.split(',') if ip.strip()]
            elif env_cluster_endpoint:
                cluster_endpoints = [env_cluster_endpoint.strip()]
            else:
                cluster_endpoints_config = config_data.get('cluster_endpoints')
                if cluster_endpoints_config:
                    if isinstance(cluster_endpoints_config, str):
                        cluster_endpoints = [cluster_endpoints_config.strip()] if cluster_endpoints_config.strip() else []
                    elif isinstance(cluster_endpoints_config, list):
                        cluster_endpoints = [str(ip).strip() for ip in cluster_endpoints_config if str(ip).strip()]
                    else:
                        raise ConfigError("cluster_endpoints must be a list or string if provided.")
                else:
                    legacy_endpoint = config_data.get('cluster_endpoint')
                    if legacy_endpoint is not None:
                        legacy_endpoint_str = str(legacy_endpoint).strip()
                        cluster_endpoints = [legacy_endpoint_str] if legacy_endpoint_str else []
                    else:
                        cluster_endpoints = []
            scrape_duration = int(os.getenv('JMX_SCRAPE_DURATION', config_data.get('scrape_duration', 60)))
            port = int(os.getenv('JMX_EXPORTER_PORT', config_data.get('port', 9095)))
            scan_interval = int(os.getenv('NODE_SCAN_INTERVAL', config_data.get('scan_new_nodes_interval', 120)))
            jmx_port = int(os.getenv('JMX_PORT', config_data.get('jmx_port', 7199)))
            max_workers = int(os.getenv('MAX_WORKERS', config_data.get('max_workers', 20)))
            disable_internal_metrics_str = str(os.getenv('DISABLE_INTERNAL_METRICS', config_data.get('disable_internal_metrics', 'false'))).lower()
            disable_internal_metrics = disable_internal_metrics_str in ('true', '1', 't', 'y', 'yes')

            if not cluster_endpoints:
                raise ConfigError(
                    "At least one cluster endpoint is required. Set cluster_endpoints in the config file "
                    "or use the CASSANDRA_CLUSTER_ENDPOINTS (comma-separated) / CASSANDRA_CLUSTER_ENDPOINT env vars."
                )
            if not config_data.get('jmx_items'):
                raise ConfigError("jmx_items configuration is required in config file.")

            return cls(
                jmx_items=config_data['jmx_items'],
                cluster_endpoints=cluster_endpoints,
                scrape_duration=scrape_duration,
                port=port,
                scan_new_nodes_interval=scan_interval,
                jmx_port=jmx_port,
                max_workers=max_workers,
                disable_internal_metrics=disable_internal_metrics
            )
        except FileNotFoundError:
            raise ConfigError(f"Config file not found at {final_path}")
        except Exception as e:
            raise ConfigError(f"Error loading config: {e}")

# --- Prometheus Metrics Management ---
class MetricsManager:
    """Manages Prometheus metrics for Cassandra"""
    def __init__(self):
        self._metrics: Dict[str, Gauge] = {}
        self._cluster_name = "unknown"

    def set_cluster_name(self, name: str):
        self._cluster_name = name

    def get_or_create_metric(self, name: str, description: str) -> Gauge:
        if name not in self._metrics:
            self._metrics[name] = Gauge(
                f'cassandra_{name}_total',
                description,
                ['ip', 'cluster_name']
            )
        return self._metrics[name]

    def set_metric_value(self, name: str, ip: str, value: float):
        if name in self._metrics:
            self._metrics[name].labels(ip=ip, cluster_name=self._cluster_name).set(value)

# --- Health Check Server ---
class HealthCheck:
    """Handles service health checks"""
    def __init__(self):
        self._ready = False
        self._lock = threading.Lock()

    def is_ready(self) -> bool:
        with self._lock:
            return self._ready

    def set_ready(self, status: bool):
        with self._lock:
            self._ready = status

    def app(self, environ, start_response):
        path = environ.get('PATH_INFO', '')
        if path == '/health/live':
            status = '200 OK'
            response = b'OK'
        elif path == '/health/ready':
            if self.is_ready():
                status = '200 OK'
                response = b'Ready'
            else:
                status = '503 Service Unavailable'
                response = b'Not Ready'
        else:
            status = '404 Not Found'
            response = b'Not Found'

        headers = [('Content-type', 'text/plain')]
        start_response(status, headers)
        return [response]

# --- JMX Connection Cache ---
class JMXConnectionCache:
    """
    Thread-safe cache for JMX connections with TTL + max-size eviction.

    IMPORTANT:
    - Never hold the lock while doing slow operations (creating connections, closing connections).
    - Uses double-checked insertion to avoid duplicate connections under concurrency.
    """

    def __init__(self):
        self.max_size = int(os.getenv('JMX_CONN_CACHE_MAX_SIZE', '50'))
        self.ttl_seconds = int(os.getenv('JMX_CONN_CACHE_TTL_SECONDS', '120'))
        self._lock = threading.Lock()
        # jmx_url -> (conn, last_used)
        self._cache: Dict[str, Tuple[object, float]] = {}

    def _safe_close(self, conn) -> None:
        try:
            close_fn = getattr(conn, "close", None)
            if callable(close_fn):
                close_fn()
        except Exception:
            logger.debug("Ignoring exception while closing JMX connection", exc_info=True)

    def _is_expired(self, last_used: float, now: float) -> bool:
        return (now - last_used) > self.ttl_seconds

    def _evict_one_locked(self) -> Optional[object]:
        """Evict one entry (LRU by last_used). Must be called with lock held.
        Returns the evicted connection object (to close outside the lock) or None.
        """
        if not self._cache:
            return None
        oldest_key = min(self._cache.items(), key=lambda kv: kv[1][1])[0]
        conn, _ = self._cache.pop(oldest_key)
        return conn

    def get(self, jmx_url: str, connect_timeout: int = 10):
        now = time.time()

        # Fast path: try return cached conn without doing slow work
        with self._lock:
            entry = self._cache.get(jmx_url)
            if entry is not None:
                conn, last_used = entry
                if not self._is_expired(last_used, now):
                    self._cache[jmx_url] = (conn, now)
                    return conn
                # expired: remove from cache; close outside lock
                self._cache.pop(jmx_url, None)
                expired_conn = conn
            else:
                expired_conn = None

        # Close expired outside lock
        if expired_conn is not None:
            self._safe_close(expired_conn)

        # Create connection outside lock (slow / may hang)
        new_conn = create_jmx_connection(jmx_url, timeout=connect_timeout)

        # Insert with double-check
        with self._lock:
            now2 = time.time()
            entry2 = self._cache.get(jmx_url)
            if entry2 is not None:
                conn2, last_used2 = entry2
                if not self._is_expired(last_used2, now2):
                    # Someone else created it while we were connecting
                    self._cache[jmx_url] = (conn2, now2)
                    # Close the redundant connection we created
                    redundant = new_conn
                    new_conn = None
                    conn_to_return = conn2
                else:
                    # Existing is expired; remove and replace
                    self._cache.pop(jmx_url, None)
                    conn_to_return = None
                    redundant = conn2
            else:
                conn_to_return = None
                redundant = None

            # Close any redundant/expired existing outside lock later
            to_close = []
            if redundant is not None:
                to_close.append(redundant)

            if new_conn is not None:
                # ensure space
                while len(self._cache) >= self.max_size:
                    evicted = self._evict_one_locked()
                    if evicted is None:
                        break
                    to_close.append(evicted)

                self._cache[jmx_url] = (new_conn, now2)
                conn_to_return = new_conn

        # Close evicted/replaced connections outside lock
        for c in to_close:
            self._safe_close(c)

        return conn_to_return

    def invalidate(self, jmx_url: str) -> None:
        """Remove a single connection from cache and close it (close happens outside lock)."""
        with self._lock:
            entry = self._cache.pop(jmx_url, None)
        if entry is not None:
            conn, _ = entry
            self._safe_close(conn)

    def close_all(self) -> None:
        """Close all connections without holding the lock during close()."""
        with self._lock:
            conns = [conn for (conn, _) in self._cache.values()]
            self._cache.clear()
        for c in conns:
            self._safe_close(c)

# --- Core JMX Monitoring Logic ---
class JMXMonitor:
    def __init__(self, config: Config, metrics_manager: MetricsManager, internal_metrics: Any, health_check: HealthCheck):
        self.config = config
        self.metrics_manager = metrics_manager
        self.internal_metrics = internal_metrics
        self.health_check = health_check
        self.cluster_node_list: List[str] = []
        self._shutdown = threading.Event()
        self._lock = threading.Lock()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_workers)
        self._local_jmx_warning_logged = False
        self._node_consecutive_timeouts: Dict[str, int] = {}
        self._node_skip_until: Dict[str, float] = {}
        self._timeout_threshold = 3
        self._skip_cooldown = 60
        self._discovery_thread = None
        self._jmx_cache = JMXConnectionCache()

    def _is_loopback_connection_error(self, exception: Exception) -> bool:
        """Detects when the JMX connection is redirected to a loopback address."""
        message = str(exception)
        if not message:
            return False
        message_lower = message.lower()
        return 'connection refused' in message_lower and '127.0.0.1' in message

    def _build_jmx_url(self, ip: str) -> str:
        """Build JMX URL for a given IP."""
        return f"service:jmx:rmi:///jndi/rmi://{ip}:{self.config.jmx_port}/jmxrmi"

    def _classify_exception(self, exception: Exception) -> str:
        message = str(exception).lower()
        if isinstance(exception, (TimeoutError, socket.timeout)):
            return 'timeout'
        if 'timed out' in message:
            return 'timeout'
        if 'parse' in message or 'format' in message:
            return 'parse'
        if 'jmx' in message:
            return 'jmx'
        return 'other'

    def _record_node_error(self, ip: str, exception: Exception = None, error_type: str = None):
        if error_type is None:
            error_type = self._classify_exception(exception) if exception else 'other'
        self.internal_metrics.node_scrape_errors_total.labels(ip=ip).inc()
        self.internal_metrics.node_errors_total.labels(ip=ip, error_type=error_type).inc()
        if error_type == 'timeout':
            self.internal_metrics.node_timeouts_total.labels(ip=ip).inc()

    def _collect_from_single_node(self, ip: str):
        """
        Performs a single metric collection from one node.
        This function is designed to be run in a thread pool.
        """
        try:
            jmx_url = f"service:jmx:rmi:///jndi/rmi://{ip}:{self.config.jmx_port}/jmxrmi"
            connect_start = time.time()
            try:
                jmx_conn = self._jmx_cache.get(jmx_url)
            except Exception as e:
                connect_duration = time.time() - connect_start
                self.internal_metrics.node_connect_duration_seconds.labels(ip=ip).observe(connect_duration)
                raise

            connect_duration = time.time() - connect_start
            self.internal_metrics.node_connect_duration_seconds.labels(ip=ip).observe(connect_duration)

            queries = [
                jmxquery.JMXQuery(item['objectName'], item['attribute'], metric_name=name)
                for name, item in self.config.jmx_items.items()
            ]

            query_start = time.time()
            try:
                metrics = jmx_conn.query(queries)
            except Exception:
                query_duration = time.time() - query_start
                self.internal_metrics.node_query_duration_seconds.labels(ip=ip).observe(query_duration)
                raise

            query_duration = time.time() - query_start
            self.internal_metrics.node_query_duration_seconds.labels(ip=ip).observe(query_duration)

            sample_count = 0
            for metric in metrics:
                try:
                    value = float(metric.value)
                    self.metrics_manager.set_metric_value(metric.metric_name, ip, value)
                    sample_count += 1
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert metric '{metric.metric_name}' value '{metric.value}' to float for node {ip}.")
                    self._record_node_error(ip, error_type='parse')

            self.internal_metrics.node_samples_emitted.labels(ip=ip).set(sample_count)
            self.internal_metrics.node_last_success_timestamp_seconds.labels(ip=ip).set(time.time())
            with self._lock:
                self._node_consecutive_timeouts[ip] = 0
            logger.debug(f"Successfully collected metrics from {ip}")
        except Exception as e:
            logger.error(f"Error collecting metrics from {ip}: {e}")
            self._record_node_error(ip, exception=e)

    def _run_scrape_cycle(self):
        """
        Executes one full scrape cycle across all known nodes using the thread pool.
        """
        start_time = time.time()
        self.internal_metrics.scrapes_in_progress.set(1)

        try:
            with self._lock:
                all_nodes = list(self.cluster_node_list)
                
            nodes_to_scrape = []
            for ip in all_nodes:
                with self._lock:
                    skip_until = self._node_skip_until.get(ip, 0)
                    if time.time() < skip_until:
                        logger.warning(f"Skipping node {ip} due to repeated timeouts (cooldown active)")
                        self.internal_metrics.node_skipped_total.labels(ip=ip, reason='timeout').inc()
                        continue
                    nodes_to_scrape.append(ip)

            logger.info(f"Starting scrape cycle for {len(nodes_to_scrape)} nodes (out of {len(all_nodes)} discovered)...")
            self.internal_metrics.nodes_scheduled.set(len(nodes_to_scrape))

            # Submit collection tasks to the thread pool
            future_to_node = {self.executor.submit(self._collect_from_single_node, ip): ip for ip in nodes_to_scrape}
            
            # Calculate timeout budget: scrape_duration minus a small margin to allow for overhead
            budget = max(0.0, self.config.scrape_duration - 0.5)
            
            try:
                done, not_done = concurrent.futures.wait(
                    future_to_node, 
                    timeout=budget, 
                    return_when=concurrent.futures.ALL_COMPLETED
                )
                
                for future in done:
                    ip = future_to_node[future]
                    try:
                        future.result()
                    except Exception as e:
                        # Exceptions are already logged and recorded in _collect_from_single_node
                        logger.debug(f"Future for node {ip} raised an exception: {e}")

                for future in not_done:
                    ip = future_to_node[future]
                    logger.warning(f"Scrape for node {ip} timed out after {budget}s")
                    jmx_url = self._build_jmx_url(ip)
                    self._jmx_cache.invalidate(jmx_url)

                    cancelled = future.cancel()
                    if not cancelled:
                        logger.warning(f"Failed to cancel future for node {ip} (task already running)")
                        self.internal_metrics.cancel_failed_total.labels(ip=ip).inc()
                        # Immediately open cooldown/skip for this node
                        with self._lock:
                            self._node_consecutive_timeouts[ip] = self._timeout_threshold
                            self._node_skip_until[ip] = time.time() + self._skip_cooldown
                            logger.error(f"Node {ip} severe timeout (cancel failed). Skipping for {self._skip_cooldown}s.")
                    else:
                        # Normal timeout, increment consecutive
                        with self._lock:
                            self._node_consecutive_timeouts[ip] = self._node_consecutive_timeouts.get(ip, 0) + 1
                            if self._node_consecutive_timeouts[ip] >= self._timeout_threshold:
                                self._node_skip_until[ip] = time.time() + self._skip_cooldown
                                logger.error(f"Node {ip} reached {self._timeout_threshold} consecutive timeouts. Skipping for {self._skip_cooldown}s.")

                    self._record_node_error(ip, error_type='timeout')
                    # Record a duration for the timed out node to ensure metrics are consistent
                    self.internal_metrics.node_query_duration_seconds.labels(ip=ip).observe(budget)

            except Exception as e:
                logger.error(f"Error during scrape cycle execution: {e}")

        finally:
            self.internal_metrics.scrapes_in_progress.set(0)

        duration = time.time() - start_time
        self.internal_metrics.scrape_duration_seconds.set(duration)
        self.internal_metrics.scrapes_total.inc()
        logger.info(f"Scrape cycle finished in {duration:.2f} seconds.")

    def _discover_nodes(self) -> List[str]:
        """Discovers Cassandra nodes by iterating over configured cluster endpoints."""
        logger.info(
            "Discovering nodes using endpoints: %s",
            ", ".join(self.config.cluster_endpoints)
        )

        for endpoint in self.config.cluster_endpoints:
            logger.info(f"Attempting discovery from endpoint {endpoint}...")
            attempt_start = time.time()
            try:
                jmx_url = f"service:jmx:rmi:///jndi/rmi://{endpoint}:{self.config.jmx_port}/jmxrmi"
                conn = create_jmx_connection(jmx_url)
                query = [jmxquery.JMXQuery("org.apache.cassandra.net:type=FailureDetector", "SimpleStates")]
                metrics = conn.query(query)

                nodes = []
                raw_states = metrics[0].value if metrics and metrics[0].value else ""
                for node_state in raw_states.split(','):
                    if 'UP' in node_state:
                        # Regex to extract IP address from formats like '/10.0.0.1=UP'
                        match = re.search(r'/([\d\.]+)=UP', node_state)
                        if match:
                            nodes.append(match.group(1))
                self.internal_metrics.discovery_duration_seconds.observe(time.time() - attempt_start)
                logger.info(f"Discovery via {endpoint} found {len(nodes)} UP nodes.")
                return nodes
            except Exception as e:
                logger.warning(f"Error discovering nodes from {endpoint}: {e}")
                self.internal_metrics.discovery_duration_seconds.observe(time.time() - attempt_start)
                self.internal_metrics.discovery_errors_total.inc()
                if self._is_loopback_connection_error(e) and not self._local_jmx_warning_logged:
                    logger.error(
                        "JMX endpoint %s redirected the connection to 127.0.0.1. "
                        "This usually means Cassandra is running with LOCAL_JMX=yes or is otherwise "
                        "advertising a loopback address. Configure Cassandra to allow remote JMX connections "
                        "(for example, set LOCAL_JMX=no and java.rmi.server.hostname to the pod IP) so that the "
                        "exporter can reach it.",
                        endpoint
                    )
                    self._local_jmx_warning_logged = True

        logger.error("All configured cluster endpoints failed during discovery.")
        return []

    def _get_cluster_name(self, ip: str) -> str:
        """Gets cluster name from any available node."""
        try:
            jmx_url = f"service:jmx:rmi:///jndi/rmi://{ip}:{self.config.jmx_port}/jmxrmi"
            jmx_conn = create_jmx_connection(jmx_url)
            query = jmxquery.JMXQuery("org.apache.cassandra.db:type=StorageService", "ClusterName")
            result = jmx_conn.query([query])
            return result[0].value if result else "unknown"
        except Exception as e:
            logger.error(f"Error getting cluster name from {ip}: {e}")
            return "unknown"

    def _update_node_list(self):
        """Periodically checks for new nodes and updates the monitored list."""
        while not self._shutdown.is_set():
            try:
                current_nodes = self._discover_nodes()
                with self._lock:
                    if set(current_nodes) != set(self.cluster_node_list):
                        logger.info(f"Node list changed. Old: {len(self.cluster_node_list)}, New: {len(current_nodes)}")
                        self.cluster_node_list = current_nodes
                        self.internal_metrics.nodes_discovered.set(len(current_nodes))
            except Exception as e:
                if not self._shutdown.is_set():
                    logger.error(f"Error in node discovery loop: {e}")
            
            self._shutdown.wait(self.config.scan_new_nodes_interval)

    def start(self):
        """Starts the monitoring service."""
        logger.info("JMX Monitor is starting...")
        
        # Initial node discovery
        self.cluster_node_list = self._discover_nodes()
        if not self.cluster_node_list:
            logger.warning("Initial node discovery failed. Will retry.")
        self.internal_metrics.nodes_discovered.set(len(self.cluster_node_list))

        # Get cluster name and create metrics
        if self.cluster_node_list:
            cluster_name = self._get_cluster_name(self.cluster_node_list[0])
            self.metrics_manager.set_cluster_name(cluster_name)
            logger.info(f"Set cluster name to: {cluster_name}")
            
        for name, item in self.config.jmx_items.items():
            self.metrics_manager.get_or_create_metric(name, f"JMX: {item['objectName']} - {item['attribute']}")

        # Start node discovery in a separate thread
        self._discovery_thread = threading.Thread(target=self._update_node_list, daemon=True, name='node-discovery')
        self._discovery_thread.start()
        
        self.health_check.set_ready(True)
        logger.info("Service is ready.")

        # Main scrape loop
        while not self._shutdown.is_set():
            self._run_scrape_cycle()
            self._shutdown.wait(self.config.scrape_duration)

    def shutdown(self):
        """Gracefully shuts down the service."""
        logger.info("Shutting down JMX monitor...")
        self.health_check.set_ready(False)
        self._shutdown.set()

        t = getattr(self, "_discovery_thread", None)
        if t and t.is_alive():
            t.join(timeout=2.0)

        # Close all cached JMX connections
        self._jmx_cache.close_all()

        # Use wait=False to avoid hanging on stuck threads during shutdown.
        # Stuck threads will be terminated when the main process exits.
        self.executor.shutdown(wait=False)
        logger.info("Shutdown complete.")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='JMX Exporter for Cassandra')
    parser.add_argument('--config', '-c', help='Path to configuration file. Can also be set via CONFIG_PATH env var.')
    parser.add_argument('--version', '-v', action='version', version=f'JMX Exporter version {VERSION}')
    args = parser.parse_args()

    jmx_monitor = None
    try:
        logger.info(f"Starting JMX Exporter version {VERSION}")
        
        config = Config.load(args.config)
        metrics_manager = MetricsManager()
        
        if config.disable_internal_metrics:
            logger.info("Internal metrics are disabled.")
            internal_metrics = NoOpMetrics()
        else:
            internal_metrics = InternalMetrics()
            internal_metrics.info.info({'version': VERSION})

        health_check = HealthCheck()

        # Start Prometheus and Health Check servers in threads
        from prometheus_client import start_http_server
        start_http_server(config.port)
        logger.info(f"Prometheus metrics server started on port {config.port}")

        health_server = make_server('', config.port + 1, health_check.app)
        health_thread = threading.Thread(target=health_server.serve_forever, daemon=True, name='health-server')
        health_thread.start()
        logger.info(f"Health check server started on port {config.port + 1}")
        
        jmx_monitor = JMXMonitor(config, metrics_manager, internal_metrics, health_check)

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            if jmx_monitor:
                jmx_monitor.shutdown()
            health_server.shutdown()
            sys.exit(0)

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        jmx_monitor.start()

    except ConfigError as e:
        logger.critical(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"A fatal error occurred: {e}", exc_info=True)
        if jmx_monitor:
            jmx_monitor.shutdown()
        sys.exit(1)

if __name__ == '__main__':
    main()

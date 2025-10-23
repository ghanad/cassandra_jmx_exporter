#!/usr/bin/python3
import logging
import socket
import time
import jmxquery
import threading
from prometheus_client import Counter, Gauge, Info
import re
import os
import yaml
import sys
import signal
import argparse
from typing import Dict, List, Any
from dataclasses import dataclass, field
import json
from wsgiref.simple_server import make_server
import concurrent.futures

VERSION = "1.5.0"

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
        self.nodes_discovered = Gauge(
            'jmx_exporter_nodes_discovered',
            'The current number of Cassandra nodes being monitored.'
        )
        self.node_scrape_errors_total = Counter(
            'jmx_exporter_node_scrape_errors_total',
            'Total number of scrape errors per node.',
            ['ip']
        )

# --- Configuration Management ---
@dataclass
class Config:
    """Configuration class supporting both YAML and environment variables"""
    jmx_items: Dict[str, Any]
    cluster_endpoint: str
    scrape_duration: int
    port: int
    scan_new_nodes_interval: int
    jmx_port: int = 7199
    max_workers: int = 20

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

            cluster_endpoint = os.getenv('CASSANDRA_CLUSTER_ENDPOINT', config_data.get('cluster_endpoint'))
            scrape_duration = int(os.getenv('JMX_SCRAPE_DURATION', config_data.get('scrape_duration', 60)))
            port = int(os.getenv('JMX_EXPORTER_PORT', config_data.get('port', 9095)))
            scan_interval = int(os.getenv('NODE_SCAN_INTERVAL', config_data.get('scan_new_nodes_interval', 120)))
            jmx_port = int(os.getenv('JMX_PORT', config_data.get('jmx_port', 7199)))
            max_workers = int(os.getenv('MAX_WORKERS', config_data.get('max_workers', 20)))

            if not cluster_endpoint:
                raise ConfigError("cluster_endpoint is required in config file or as CASSANDRA_CLUSTER_ENDPOINT env var.")
            if not config_data.get('jmx_items'):
                raise ConfigError("jmx_items configuration is required in config file.")

            return cls(
                jmx_items=config_data['jmx_items'],
                cluster_endpoint=cluster_endpoint,
                scrape_duration=scrape_duration,
                port=port,
                scan_new_nodes_interval=scan_interval,
                jmx_port=jmx_port,
                max_workers=max_workers
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
            self._metrics[name] = Gauge(f'cassandra_{name}', description, ['ip', 'cluster'])
        return self._metrics[name]

    def set_metric_value(self, name: str, ip: str, value: float):
        if name in self._metrics:
            self._metrics[name].labels(ip=ip, cluster=self._cluster_name).set(value)

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

# --- Core JMX Monitoring Logic ---
class JMXMonitor:
    def __init__(self, config: Config, metrics_manager: MetricsManager, internal_metrics: InternalMetrics, health_check: HealthCheck):
        self.config = config
        self.metrics_manager = metrics_manager
        self.internal_metrics = internal_metrics
        self.health_check = health_check
        self.cluster_node_list: List[str] = []
        self._shutdown = threading.Event()
        self._lock = threading.Lock()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_workers)

    def _collect_from_single_node(self, ip: str):
        """
        Performs a single metric collection from one node.
        This function is designed to be run in a thread pool.
        """
        try:
            jmx_url = f"service:jmx:rmi:///jndi/rmi://{ip}:{self.config.jmx_port}/jmxrmi"
            jmx_conn = jmxquery.JMXConnection(jmx_url, timeout=10)
            
            queries = [
                jmxquery.JMXQuery(item['objectName'], item['attribute'], metric_name=name)
                for name, item in self.config.jmx_items.items()
            ]
            metrics = jmx_conn.query(queries)
            
            for metric in metrics:
                try:
                    value = float(metric.value)
                    self.metrics_manager.set_metric_value(metric.metric_name, ip, value)
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert metric '{metric.metric_name}' value '{metric.value}' to float for node {ip}.")
            
            logger.debug(f"Successfully collected metrics from {ip}")
        except Exception as e:
            logger.error(f"Error collecting metrics from {ip}: {e}")
            self.internal_metrics.node_scrape_errors_total.labels(ip=ip).inc()

    def _run_scrape_cycle(self):
        """
        Executes one full scrape cycle across all known nodes using the thread pool.
        """
        logger.info(f"Starting scrape cycle for {len(self.cluster_node_list)} nodes...")
        start_time = time.time()

        with self._lock:
            nodes_to_scrape = list(self.cluster_node_list)

        # Submit collection tasks to the thread pool
        future_to_node = {self.executor.submit(self._collect_from_single_node, ip): ip for ip in nodes_to_scrape}
        concurrent.futures.wait(future_to_node)
        
        duration = time.time() - start_time
        self.internal_metrics.scrape_duration_seconds.set(duration)
        self.internal_metrics.scrapes_total.inc()
        logger.info(f"Scrape cycle finished in {duration:.2f} seconds.")

    def _discover_nodes(self) -> List[str]:
        """Discovers Cassandra nodes from the cluster endpoint."""
        logger.info(f"Discovering nodes from endpoint {self.config.cluster_endpoint}...")
        try:
            jmx_url = f"service:jmx:rmi:///jndi/rmi://{self.config.cluster_endpoint}:{self.config.jmx_port}/jmxrmi"
            conn = jmxquery.JMXConnection(jmx_url, timeout=10)
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
            logger.info(f"Discovery found {len(nodes)} UP nodes.")
            return nodes
        except Exception as e:
            logger.error(f"Error discovering nodes: {e}")
            return []

    def _get_cluster_name(self, ip: str) -> str:
        """Gets cluster name from any available node."""
        try:
            jmx_url = f"service:jmx:rmi:///jndi/rmi://{ip}:{self.config.jmx_port}/jmxrmi"
            jmx_conn = jmxquery.JMXConnection(jmx_url, timeout=10)
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
                logger.error(f"Error in node discovery loop: {e}")
            
            time.sleep(self.config.scan_new_nodes_interval)

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
        discovery_thread = threading.Thread(target=self._update_node_list, daemon=True, name='node-discovery')
        discovery_thread.start()
        
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
        self.executor.shutdown(wait=True)
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
        internal_metrics = InternalMetrics()
        health_check = HealthCheck()
        
        internal_metrics.info.info({'version': VERSION})

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

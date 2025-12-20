#!/usr/bin/python3
# -*- coding: utf-8 -*-
# jmx_monitoring_central_optimized.py

import argparse
import concurrent.futures
import inspect
import logging
import os
import re
import signal
import socket
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from wsgiref.simple_server import make_server
import zlib

import yaml
import jmxquery
from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server

VERSION = "1.2.0-central-optimized"

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class JMXExporterError(Exception):
    pass


class ConfigError(JMXExporterError):
    pass


_JMX_CONNECTION_SUPPORTS_TIMEOUT = "timeout" in inspect.signature(jmxquery.JMXConnection).parameters


def create_jmx_connection(jmx_url: str, timeout: int = 10) -> jmxquery.JMXConnection:
    if _JMX_CONNECTION_SUPPORTS_TIMEOUT:
        return jmxquery.JMXConnection(jmx_url, timeout=timeout)
    logger.debug("jmxquery.JMXConnection does not support timeout parameter; creating connection without it.")
    return jmxquery.JMXConnection(jmx_url)


class NoOpMetric:
    def inc(self, *args: Any, **kwargs: Any) -> None:
        return

    def set(self, *args: Any, **kwargs: Any) -> None:
        return

    def observe(self, *args: Any, **kwargs: Any) -> None:
        return

    def labels(self, *args: Any, **kwargs: Any) -> "NoOpMetric":
        return self

    def info(self, *args: Any, **kwargs: Any) -> None:
        return


class InternalMetrics:
    def __init__(self):
        self.info = Info("jmx_exporter", "Information about the JMX Exporter")

        self.scrapes_total = Counter("jmx_exporter_scrapes_total", "Total number of scrape cycles completed.")
        self.scrape_duration_seconds = Gauge(
            "jmx_exporter_scrape_duration_seconds", "Duration of the last scrape cycle in seconds."
        )
        self.scrapes_in_progress = Gauge(
            "jmx_exporter_scrapes_in_progress", "Whether a scrape cycle is in progress (1) or not (0)."
        )

        self.nodes_discovered = Gauge("jmx_exporter_nodes_discovered", "Number of nodes currently discovered.")
        self.nodes_scraped = Gauge("jmx_exporter_nodes_scraped", "Nodes attempted in last scrape cycle.")
        self.nodes_skipped = Gauge(
            "jmx_exporter_nodes_skipped", "Nodes skipped (cooldown/sharding/batching) in last scrape cycle."
        )

        self.discovery_duration_seconds = Histogram(
            "jmx_exporter_discovery_duration_seconds", "Duration of node discovery attempts in seconds."
        )

        # Note: per-ip labels create time series; keep it if you need it. Otherwise disable internal metrics.
        self.node_connect_duration_seconds = Histogram(
            "jmx_exporter_node_connect_duration_seconds", "JMX connect duration per node in seconds.", ["ip"]
        )
        self.node_query_duration_seconds = Histogram(
            "jmx_exporter_node_query_duration_seconds", "JMX query duration per node in seconds.", ["ip"]
        )

        self.node_errors_total = Counter(
            "jmx_exporter_node_errors_total", "Total node scrape errors by type.", ["ip", "error_type"]
        )
        self.scrape_timeouts_total = Counter(
            "jmx_exporter_scrape_timeouts_total", "Number of node scrape timeouts.", ["ip"]
        )
        self.cancel_failed_total = Counter(
            "jmx_exporter_cancel_failed_total", "Futures that could not be cancelled after timeout.", ["ip"]
        )

        self.cache_hits_total = Counter("jmx_exporter_cache_hits_total", "Connection cache hits.", ["jmx_url"])
        self.cache_misses_total = Counter("jmx_exporter_cache_misses_total", "Connection cache misses.", ["jmx_url"])
        self.cache_evictions_total = Counter("jmx_exporter_cache_evictions_total", "Connection cache evictions.")
        self.cache_expired_total = Counter("jmx_exporter_cache_expired_total", "Connection cache expirations.")

        self.last_node_scrape_timestamp_seconds = Gauge(
            "jmx_exporter_last_node_scrape_timestamp_seconds",
            "Unix timestamp of last successful scrape per node.",
            ["ip"],
        )


class NoOpMetrics:
    def __init__(self):
        noop = NoOpMetric()
        for k in [
            "info",
            "scrapes_total",
            "scrape_duration_seconds",
            "scrapes_in_progress",
            "nodes_discovered",
            "nodes_scraped",
            "nodes_skipped",
            "discovery_duration_seconds",
            "node_connect_duration_seconds",
            "node_query_duration_seconds",
            "node_errors_total",
            "scrape_timeouts_total",
            "cancel_failed_total",
            "cache_hits_total",
            "cache_misses_total",
            "cache_evictions_total",
            "cache_expired_total",
            "last_node_scrape_timestamp_seconds",
        ]:
            setattr(self, k, noop)


class HealthCheck:
    def __init__(self):
        self._ready = False
        self._lock = threading.Lock()

    def set_ready(self, ready: bool):
        with self._lock:
            self._ready = bool(ready)

    def is_ready(self) -> bool:
        with self._lock:
            return self._ready

    def app(self, environ, start_response):
        path = environ.get("PATH_INFO", "")
        if path == "/health/live":
            start_response("200 OK", [("Content-Type", "text/plain")])
            return [b"OK"]
        if path == "/health/ready":
            if self.is_ready():
                start_response("200 OK", [("Content-Type", "text/plain")])
                return [b"Ready"]
            start_response("503 Service Unavailable", [("Content-Type", "text/plain")])
            return [b"Not Ready"]
        start_response("404 Not Found", [("Content-Type", "text/plain")])
        return [b"Not Found"]


@dataclass
class Config:
    jmx_items: Dict[str, Any]
    cluster_endpoints: List[str]
    scrape_duration: int
    port: int
    scan_new_nodes_interval: int
    jmx_port: int = 7199
    max_workers: int = 20
    disable_internal_metrics: bool = False

    # Optimizations knobs
    jmx_connect_timeout: int = 10
    shard_total: int = 1
    shard_id: int = 0
    scrape_batch_size: int = 0  # 0 => all nodes

    @classmethod
    def load(cls, config_path: str = None) -> "Config":
        final_path = config_path or os.getenv("CONFIG_PATH", "/etc/jmx-exporter/config.yml")
        logger.info("Loading configuration from: %s", final_path)

        try:
            with open(final_path, encoding="utf-8") as f:
                config_data = yaml.safe_load(f) or {}
        except FileNotFoundError as e:
            raise ConfigError(f"Configuration file not found: {e}") from e
        except yaml.YAMLError as e:
            raise ConfigError(f"Error parsing YAML config file: {e}") from e

        env_cluster_endpoints = os.getenv("CASSANDRA_CLUSTER_ENDPOINTS")
        env_cluster_endpoint = os.getenv("CASSANDRA_CLUSTER_ENDPOINT")

        if env_cluster_endpoints:
            cluster_endpoints = [x.strip() for x in env_cluster_endpoints.split(",") if x.strip()]
        elif env_cluster_endpoint:
            cluster_endpoints = [env_cluster_endpoint.strip()]
        else:
            ce = config_data.get("cluster_endpoints")
            if ce is not None:
                if isinstance(ce, str):
                    cluster_endpoints = [x.strip() for x in ce.split(",") if x.strip()]
                elif isinstance(ce, list):
                    cluster_endpoints = [str(x).strip() for x in ce if str(x).strip()]
                else:
                    raise ConfigError("cluster_endpoints must be a list or string if provided.")
            else:
                legacy = config_data.get("cluster_endpoint")
                cluster_endpoints = [str(legacy).strip()] if legacy else []

        if not cluster_endpoints:
            raise ConfigError(
                "At least one cluster endpoint is required. Use cluster_endpoints in YAML or "
                "CASSANDRA_CLUSTER_ENDPOINTS / CASSANDRA_CLUSTER_ENDPOINT env vars."
            )
        if not config_data.get("jmx_items"):
            raise ConfigError("jmx_items configuration is required in config file.")

        def _int(name: str, default: int) -> int:
            v = os.getenv(name)
            if v is None:
                return default
            return int(v)

        scrape_duration = _int("JMX_SCRAPE_DURATION", int(config_data.get("scrape_duration", 60)))
        port = _int("JMX_EXPORTER_PORT", int(config_data.get("port", 9095)))
        scan_interval = _int("NODE_SCAN_INTERVAL", int(config_data.get("scan_new_nodes_interval", 120)))
        jmx_port = _int("JMX_PORT", int(config_data.get("jmx_port", 7199)))
        max_workers = _int("MAX_WORKERS", int(config_data.get("max_workers", 20)))

        jmx_connect_timeout = _int("JMX_CONNECT_TIMEOUT", int(config_data.get("jmx_connect_timeout", 10)))
        shard_total = _int("SHARD_TOTAL", int(config_data.get("shard_total", 1)))
        shard_id = _int("SHARD_ID", int(config_data.get("shard_id", 0)))
        scrape_batch_size = _int("SCRAPE_BATCH_SIZE", int(config_data.get("scrape_batch_size", 0)))

        disable_internal_metrics_str = str(
            os.getenv("DISABLE_INTERNAL_METRICS", config_data.get("disable_internal_metrics", "false"))
        ).lower()
        disable_internal_metrics = disable_internal_metrics_str in ("true", "1", "t", "y", "yes")

        if scrape_duration <= 0:
            raise ConfigError("scrape_duration must be > 0")
        if scan_interval <= 0:
            raise ConfigError("scan_new_nodes_interval must be > 0")
        if max_workers <= 0:
            raise ConfigError("max_workers must be > 0")
        if jmx_connect_timeout <= 0:
            raise ConfigError("jmx_connect_timeout must be > 0")
        if shard_total <= 0:
            raise ConfigError("shard_total must be >= 1")
        if shard_id < 0 or shard_id >= shard_total:
            raise ConfigError("shard_id must be in range [0, shard_total-1]")
        if scrape_batch_size < 0:
            raise ConfigError("scrape_batch_size must be >= 0")

        return cls(
            jmx_items=config_data["jmx_items"],
            cluster_endpoints=cluster_endpoints,
            scrape_duration=scrape_duration,
            port=port,
            scan_new_nodes_interval=scan_interval,
            jmx_port=jmx_port,
            max_workers=max_workers,
            disable_internal_metrics=disable_internal_metrics,
            jmx_connect_timeout=jmx_connect_timeout,
            shard_total=shard_total,
            shard_id=shard_id,
            scrape_batch_size=scrape_batch_size,
        )


class MetricsManager:
    def __init__(self):
        self._metrics: Dict[str, Gauge] = {}
        self._cluster_name = "unknown"
        self._lock = threading.Lock()

    def set_cluster_name(self, name: str):
        self._cluster_name = name

    def get_or_create_metric(self, name: str, description: str) -> Gauge:
        with self._lock:
            if name not in self._metrics:
                self._metrics[name] = Gauge(
                    f"cassandra_{name}_total",
                    description,
                    ["ip", "cluster_name"],
                )
            return self._metrics[name]

    def set_metric_value(self, metric_name: str, ip: str, value: float):
        metric = self._metrics.get(metric_name)
        if metric is None:
            metric = self.get_or_create_metric(metric_name, metric_name)
        metric.labels(ip=ip, cluster_name=self._cluster_name).set(value)


class JMXConnectionCache:
    def __init__(self, internal_metrics: Optional[InternalMetrics] = None):
        self.max_size = int(os.getenv("JMX_CONN_CACHE_MAX_SIZE", "50"))
        self.ttl_seconds = int(os.getenv("JMX_CONN_CACHE_TTL_SECONDS", "120"))
        self._lock = threading.Lock()
        self._cache: Dict[str, Tuple[object, float]] = {}
        self._metrics = internal_metrics

    def _safe_close(self, conn_obj: object):
        try:
            close_fn = getattr(conn_obj, "close", None)
            if callable(close_fn):
                close_fn()
        except Exception:
            logger.debug("Ignoring exception while closing JMX connection", exc_info=True)

    def _is_expired(self, last_used: float, now: float) -> bool:
        return (now - last_used) > self.ttl_seconds

    def _evict_one_locked(self) -> Optional[Tuple[str, object]]:
        if not self._cache:
            return None
        oldest_key = min(self._cache.items(), key=lambda kv: kv[1][1])[0]
        conn, _ = self._cache.pop(oldest_key)
        return oldest_key, conn

    def get(self, jmx_url: str, connect_timeout: int = 10):
        now = time.time()
        with self._lock:
            entry = self._cache.get(jmx_url)
            if entry is not None:
                conn, last_used = entry
                if not self._is_expired(last_used, now):
                    self._cache[jmx_url] = (conn, now)
                    if self._metrics:
                        self._metrics.cache_hits_total.labels(jmx_url=jmx_url).inc()
                    return conn
                self._cache.pop(jmx_url, None)
                expired_conn = conn
                if self._metrics:
                    self._metrics.cache_expired_total.inc()
            else:
                expired_conn = None
                if self._metrics:
                    self._metrics.cache_misses_total.labels(jmx_url=jmx_url).inc()

        if expired_conn is not None:
            self._safe_close(expired_conn)

        new_conn = create_jmx_connection(jmx_url, timeout=connect_timeout)

        close_new = False
        evicted: Optional[object] = None
        with self._lock:
            now2 = time.time()
            entry2 = self._cache.get(jmx_url)
            if entry2 is not None:
                conn2, last_used2 = entry2
                if not self._is_expired(last_used2, now2):
                    self._cache[jmx_url] = (conn2, now2)
                    close_new = True
                else:
                    self._cache[jmx_url] = (new_conn, now2)
            else:
                self._cache[jmx_url] = (new_conn, now2)

            if len(self._cache) > self.max_size:
                ev = self._evict_one_locked()
                if ev is not None:
                    _, evicted = ev
                    if self._metrics:
                        self._metrics.cache_evictions_total.inc()

        if close_new:
            self._safe_close(new_conn)
            return conn2  # type: ignore

        if evicted is not None:
            self._safe_close(evicted)

        return new_conn

    def invalidate(self, jmx_url: str):
        with self._lock:
            entry = self._cache.pop(jmx_url, None)
        if entry:
            self._safe_close(entry[0])

    def close_all(self):
        with self._lock:
            items = list(self._cache.items())
            self._cache.clear()
        for _, (conn, _) in items:
            self._safe_close(conn)


class JMXMonitor:
    def __init__(self, config: Config, metrics_manager: MetricsManager, internal_metrics, health_check: HealthCheck):
        self.config = config
        self.metrics_manager = metrics_manager
        self.internal_metrics = internal_metrics
        self.health_check = health_check

        self._shutdown = threading.Event()
        self._lock = threading.Lock()
        self.cluster_node_list: List[str] = []

        self._node_skip_until: Dict[str, float] = {}
        self._node_consecutive_errors: Dict[str, int] = {}
        self._error_threshold = int(os.getenv("NODE_ERROR_THRESHOLD", "3"))
        self._skip_cooldown = int(os.getenv("NODE_SKIP_COOLDOWN_SECONDS", "300"))

        self._rr_cursor = 0  # for batching rotation

        self._jmx_cache = JMXConnectionCache(internal_metrics if isinstance(internal_metrics, InternalMetrics) else None)

        # BIG optimization: build queries once
        self._queries = [
            jmxquery.JMXQuery(item["objectName"], item["attribute"], metric_name=name)
            for name, item in self.config.jmx_items.items()
        ]

        self._discovery_thread: Optional[threading.Thread] = None

    def _build_jmx_url(self, ip: str) -> str:
        return f"service:jmx:rmi:///jndi/rmi://{ip}:{self.config.jmx_port}/jmxrmi"

    def _hash_node(self, ip: str) -> int:
        return zlib.crc32(ip.encode("utf-8")) & 0xFFFFFFFF

    def _node_in_my_shard(self, ip: str) -> bool:
        if self.config.shard_total <= 1:
            return True
        return (self._hash_node(ip) % self.config.shard_total) == self.config.shard_id

    def _select_batch(self, nodes: List[str]) -> Tuple[List[str], int]:
        bs = self.config.scrape_batch_size
        if bs <= 0 or bs >= len(nodes):
            return nodes, 0

        nodes_sorted = sorted(nodes)
        start = self._rr_cursor % len(nodes_sorted)

        selected: List[str] = []
        i = start
        while len(selected) < bs:
            selected.append(nodes_sorted[i])
            i = (i + 1) % len(nodes_sorted)
            if i == start:
                break

        self._rr_cursor = i
        skipped = len(nodes_sorted) - len(selected)
        return selected, skipped

    def _discover_nodes(self) -> List[str]:
        logger.info("Discovering nodes using endpoints: %s", ", ".join(self.config.cluster_endpoints))

        for endpoint in self.config.cluster_endpoints:
            logger.info("Attempting discovery from endpoint %s...", endpoint)
            attempt_start = time.time()
            try:
                jmx_url = self._build_jmx_url(endpoint)
                conn = create_jmx_connection(jmx_url, timeout=self.config.jmx_connect_timeout)
                query = [jmxquery.JMXQuery("org.apache.cassandra.net:type=FailureDetector", "SimpleStates")]
                metrics = conn.query(query)

                nodes: List[str] = []
                raw_states = metrics[0].value if metrics and metrics[0].value else ""
                for node_state in str(raw_states).split(","):
                    if "UP" in node_state:
                        match = re.search(r"/([\d\.]+)=UP", node_state)
                        if match:
                            nodes.append(match.group(1))

                self.internal_metrics.discovery_duration_seconds.observe(time.time() - attempt_start)
                logger.info("Discovery via %s found %s UP nodes.", endpoint, len(nodes))
                return nodes
            except Exception as e:
                self.internal_metrics.discovery_duration_seconds.observe(time.time() - attempt_start)
                logger.warning("Error discovering nodes from %s: %s", endpoint, e)

        return []

    def _update_node_list(self):
        while not self._shutdown.is_set():
            try:
                new_nodes = self._discover_nodes()
                if new_nodes:
                    new_set = set(new_nodes)
                    with self._lock:
                        old_set = set(self.cluster_node_list)
                        if new_set != old_set:
                            self.cluster_node_list = sorted(new_set)
                            logger.info("Node list updated: %s nodes", len(self.cluster_node_list))
                with self._lock:
                    self.internal_metrics.nodes_discovered.set(len(self.cluster_node_list))
            except Exception:
                logger.exception("Unexpected error in node discovery thread")

            self._shutdown.wait(self.config.scan_new_nodes_interval)

    def _record_node_error(self, ip: str, error_type: str):
        self.internal_metrics.node_errors_total.labels(ip=ip, error_type=error_type).inc()

    def _collect_from_single_node(self, ip: str):
        jmx_url = self._build_jmx_url(ip)
        try:
            connect_start = time.time()
            conn = self._jmx_cache.get(jmx_url, connect_timeout=self.config.jmx_connect_timeout)
            self.internal_metrics.node_connect_duration_seconds.labels(ip=ip).observe(time.time() - connect_start)

            query_start = time.time()
            metrics = conn.query(self._queries)
            self.internal_metrics.node_query_duration_seconds.labels(ip=ip).observe(time.time() - query_start)

            sample_count = 0
            for metric in metrics:
                try:
                    value = float(metric.value)
                except Exception:
                    continue
                self.metrics_manager.set_metric_value(metric.metric_name, ip, value)
                sample_count += 1

            self.internal_metrics.last_node_scrape_timestamp_seconds.labels(ip=ip).set(time.time())

            if sample_count == 0:
                logger.debug("Node %s returned 0 samples", ip)

            with self._lock:
                self._node_consecutive_errors[ip] = 0

        except Exception as e:
            self._jmx_cache.invalidate(jmx_url)

            msg = str(e).lower()
            if "timeout" in msg or "timed out" in msg:
                self._record_node_error(ip, "timeout")
            elif "connection refused" in msg:
                self._record_node_error(ip, "connection_refused")
            elif "127.0.0.1" in msg or "localhost" in msg:
                self._record_node_error(ip, "rmi_loopback")
            else:
                self._record_node_error(ip, "other")

            with self._lock:
                self._node_consecutive_errors[ip] = self._node_consecutive_errors.get(ip, 0) + 1
                if self._node_consecutive_errors[ip] >= self._error_threshold:
                    self._node_skip_until[ip] = time.time() + self._skip_cooldown
                    logger.error(
                        "Node %s reached %s consecutive errors. Skipping for %ss.",
                        ip,
                        self._error_threshold,
                        self._skip_cooldown,
                    )
            raise

    def _run_scrape_cycle(self):
        start_time = time.time()
        self.internal_metrics.scrapes_in_progress.set(1)

        try:
            with self._lock:
                all_nodes = list(self.cluster_node_list)
                skip_until_map = dict(self._node_skip_until)

            now = time.time()
            eligible = [ip for ip in all_nodes if now >= skip_until_map.get(ip, 0.0)]
            cooldown_skipped = len(all_nodes) - len(eligible)

            sharded = [ip for ip in eligible if self._node_in_my_shard(ip)]
            shard_skipped = len(eligible) - len(sharded)

            nodes_to_scrape, batch_skipped = self._select_batch(sharded)

            skipped_total = cooldown_skipped + shard_skipped + batch_skipped
            self.internal_metrics.nodes_scraped.set(len(nodes_to_scrape))
            self.internal_metrics.nodes_skipped.set(skipped_total)

            if not nodes_to_scrape:
                logger.warning("No nodes to scrape this cycle (all skipped or none discovered).")
                return

            budget = max(0.0, self.config.scrape_duration - 0.5)

            future_to_node: Dict[concurrent.futures.Future, str] = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                for ip in nodes_to_scrape:
                    future_to_node[executor.submit(self._collect_from_single_node, ip)] = ip

                done, not_done = concurrent.futures.wait(
                    future_to_node,
                    timeout=budget,
                    return_when=concurrent.futures.ALL_COMPLETED,
                )

                for future in not_done:
                    ip = future_to_node[future]
                    logger.warning("Scrape for node %s timed out after %ss", ip, budget)
                    self.internal_metrics.scrape_timeouts_total.labels(ip=ip).inc()
                    self._jmx_cache.invalidate(self._build_jmx_url(ip))

                    cancelled = future.cancel()
                    if not cancelled:
                        self.internal_metrics.cancel_failed_total.labels(ip=ip).inc()

                    self._record_node_error(ip, "timeout")
                    self.internal_metrics.node_query_duration_seconds.labels(ip=ip).observe(budget)

        finally:
            self.internal_metrics.scrapes_total.inc()
            self.internal_metrics.scrapes_in_progress.set(0)
            self.internal_metrics.scrape_duration_seconds.set(time.time() - start_time)

    def start(self):
        logger.info("JMX Monitor is starting...")

        self.cluster_node_list = self._discover_nodes()
        self.internal_metrics.nodes_discovered.set(len(self.cluster_node_list))
        if not self.cluster_node_list:
            logger.warning("Initial node discovery failed. Will retry in background.")

        # Best-effort cluster name
        try:
            seed = self.config.cluster_endpoints[0]
            jmx_url = self._build_jmx_url(seed)
            conn = create_jmx_connection(jmx_url, timeout=self.config.jmx_connect_timeout)
            q = [jmxquery.JMXQuery("org.apache.cassandra.db:type=StorageService", "ClusterName")]
            res = conn.query(q)
            cluster_name = str(res[0].value) if res else "unknown"
            self.metrics_manager.set_cluster_name(cluster_name)
            logger.info("Set cluster name to: %s", cluster_name)
        except Exception:
            logger.warning("Failed to fetch cluster name; leaving as 'unknown'", exc_info=True)

        for name, item in self.config.jmx_items.items():
            self.metrics_manager.get_or_create_metric(name, f"JMX: {item['objectName']} - {item['attribute']}")

        self._discovery_thread = threading.Thread(target=self._update_node_list, daemon=True, name="node-discovery")
        self._discovery_thread.start()

        self.health_check.set_ready(True)

        start_http_server(self.config.port)
        logger.info("Metrics server started on port %s", self.config.port)

        health_port = self.config.port + 1
        health_server = make_server("0.0.0.0", health_port, self.health_check.app)
        health_thread = threading.Thread(target=health_server.serve_forever, daemon=True, name="health-server")
        health_thread.start()
        logger.info("Health server started on port %s", health_port)

        while not self._shutdown.is_set():
            self._run_scrape_cycle()
            self._shutdown.wait(self.config.scrape_duration)

        try:
            health_server.shutdown()
        except Exception:
            logger.debug("Health server shutdown failed", exc_info=True)

    def shutdown(self):
        logger.info("Shutting down JMX monitor...")
        self.health_check.set_ready(False)
        self._shutdown.set()

        t = getattr(self, "_discovery_thread", None)
        if t and t.is_alive():
            t.join(timeout=2.0)

        self._jmx_cache.close_all()


def main():
    parser = argparse.ArgumentParser(description="Central JMX Exporter for Cassandra (optimized)")
    parser.add_argument("--config", "-c", help="Path to config.yml (or CONFIG_PATH env var).")
    parser.add_argument("--version", "-v", action="version", version=f"JMX Exporter version {VERSION}")
    args = parser.parse_args()

    jmx_monitor = None
    try:
        config = Config.load(args.config)
        metrics_manager = MetricsManager()

        if config.disable_internal_metrics:
            internal_metrics = NoOpMetrics()
        else:
            internal_metrics = InternalMetrics()
            internal_metrics.info.info(
                {
                    "version": VERSION,
                    "python_version": sys.version.split()[0],
                    "hostname": socket.gethostname(),
                    "shard_total": str(config.shard_total),
                    "shard_id": str(config.shard_id),
                    "batch_size": str(config.scrape_batch_size),
                }
            )

        health_check = HealthCheck()
        jmx_monitor = JMXMonitor(config, metrics_manager, internal_metrics, health_check)

        def _signal_handler(signum, frame):
            logger.info("Received signal %s. Shutting down...", signum)
            if jmx_monitor:
                jmx_monitor.shutdown()

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        jmx_monitor.start()

    except ConfigError as e:
        logger.critical("Configuration error: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.critical("Fatal error: %s", e, exc_info=True)
        if jmx_monitor:
            jmx_monitor.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    main()

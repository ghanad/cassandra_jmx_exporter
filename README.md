# JMX Exporter for Cassandra

A Python-based JMX metrics exporter for Apache Cassandra, designed for multi-cluster environments. This tool automatically discovers all nodes in a cluster by connecting to one of several configured seed endpoints and exposes their JMX metrics in Prometheus format.

## Features

-   **Auto-Discovery:** Automatically discovers all up-nodes in a Cassandra cluster.
-   **Multi-Cluster Support:** Can be configured to monitor multiple clusters by running one instance per cluster.
-   **Efficient & Lightweight:** Uses a thread pool to collect metrics concurrently, minimizing resource usage.
-   **Prometheus-Ready:** Exposes metrics in a format ready to be scraped by Prometheus.
-   **Health Checks:** Includes `/health/live` and `/health/ready` endpoints for Kubernetes integration.
-   **Docker & Kubernetes Friendly:** Comes with a `Dockerfile` and example Kubernetes manifests.
-   **Configurable:** Metrics and settings can be easily configured via a YAML file or environment variables.

## Getting Started

### Prerequisites

-   Python 3.8+
-   Docker (for containerized deployment)

### Configuration

1.  Copy `config.yml.example` to `config.yml`.
2.  Modify `config.yml` to match your environment. Key parameters are:
    -   `cluster_endpoints`: A list of seed node IPs in your Cassandra cluster. The exporter will try them in order until one is reachable.
    -   `jmx_port`: The JMX port of your Cassandra nodes (default: 7199).
    -   `jmx_items`: The list of JMX MBeans and attributes you want to collect.

### Running with Docker

1.  **Build the Docker image:**
    ```sh
    docker build -t your-repo/jmx-cassandra-exporter:latest .
    ```

2.  **Run the container:**
    ```sh
    docker run -d --rm \
      -p 9095:9095 \
      -p 9096:9096 \
      -v /path/to/your/config.yml:/etc/jmx-exporter/config.yml:ro \
      --name jmx-exporter \
      your-repo/jmx-cassandra-exporter:latest
    ```
    -   Metrics will be available at `http://localhost:9095/metrics`.
    -   Health checks will be at `http://localhost:9096/health/ready`.

## Kubernetes Deployment

Example manifests are provided in the `k8s/` directory.

A Helm chart is available under `charts/cassandra-jmx-exporter` for configurable deployments, including optional resources such as ServiceMonitor and Ingress for Prometheus scraping and external exposure.

1.  **Create a ConfigMap** from your `config.yml` file:
    ```sh
    kubectl create configmap jmx-exporter-config --from-file=config.yml -n your-namespace
    ```
2.  **Apply the deployment and service:**
    ```sh
    kubectl apply -f k8s/deployment.yaml -n your-namespace
    kubectl apply -f k8s/service.yaml -n your-namespace
    ```

## Configuration via Environment Variables

All key parameters from the config file can be overridden with environment variables for more flexible deployments.
- `CASSANDRA_CLUSTER_ENDPOINTS` (comma-separated list of seed nodes)
- `CASSANDRA_CLUSTER_ENDPOINT` (legacy single endpoint override)
- `JMX_EXPORTER_PORT`
- `JMX_PORT`
- `JMX_SCRAPE_DURATION`
- `NODE_SCAN_INTERVAL`
- `MAX_WORKERS`

## Internal Exporter Metrics

The exporter publishes a set of internal metrics that make it easier to monitor its own health, performance, and error modes. These metrics are exposed on the same `/metrics` endpoint as the Cassandra data.

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `jmx_exporter_info` | Info | `version` | Static information about the exporter build. |
| `jmx_exporter_scrapes_total` | Counter | – | Total number of completed scrape cycles. |
| `jmx_exporter_scrape_duration_seconds` | Gauge | – | Duration in seconds of the most recent scrape cycle. |
| `jmx_exporter_scrapes_in_progress` | Gauge | – | Number of scrape cycles currently running (should be 0 or 1). |
| `jmx_exporter_nodes_discovered` | Gauge | – | Number of Cassandra nodes currently scheduled for scraping. |
| `jmx_exporter_discovery_duration_seconds` | Histogram | – | Time in seconds spent performing node discovery attempts. |
| `jmx_exporter_discovery_errors_total` | Counter | – | Total number of failed discovery attempts. |
| `jmx_exporter_node_scrape_errors_total` | Counter | `ip` | Aggregate count of scrape errors for each node. |
| `jmx_exporter_node_errors_total` | Counter | `ip`, `error_type` | Categorised scrape errors per node (`timeout`, `jmx`, `parse`, `other`). |
| `jmx_exporter_node_timeouts_total` | Counter | `ip` | Number of scrape attempts per node that ended in a timeout. |
| `jmx_exporter_node_connect_duration_seconds` | Histogram | `ip` | Time spent establishing a JMX connection to a node. |
| `jmx_exporter_node_query_duration_seconds` | Histogram | `ip` | Time spent executing the JMX query against a node. |
| `jmx_exporter_node_samples_emitted` | Gauge | `ip` | Number of samples exported for a node during the last successful scrape. |
| `jmx_exporter_node_last_success_timestamp_seconds` | Gauge | `ip` | Unix timestamp for the last successful scrape of a node. |

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

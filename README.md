# JMX Exporter for Cassandra

A Python-based JMX metrics exporter for Apache Cassandra, designed for multi-cluster environments. This tool automatically discovers all nodes in a cluster by connecting to a single endpoint and exposes their JMX metrics in Prometheus format.

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
    -   `cluster_endpoint`: The IP of one node in your Cassandra cluster.
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
- `CASSANDRA_CLUSTER_ENDPOINT`
- `JMX_EXPORTER_PORT`
- `JMX_PORT`
- `JMX_SCRAPE_DURATION`
- `NODE_SCAN_INTERVAL`
- `MAX_WORKERS`

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

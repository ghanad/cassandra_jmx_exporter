# Use an official lightweight Python image as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Create a non-root user for better security
RUN useradd --no-create-home appuser

# Copy the dependencies file first to leverage Docker's layer caching
COPY --chown=appuser:appuser requirements.txt .

# Install system dependencies required by the application
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY --chown=appuser:appuser jmx_monitoring.py .

# Switch to the non-root user
USER appuser

# Expose the port the app runs on for Prometheus and health checks
EXPOSE 9095
EXPOSE 9096

# Define the command to run your app
CMD ["python3", "jmx_monitoring.py", "--config", "/etc/jmx-exporter/config.yml"]

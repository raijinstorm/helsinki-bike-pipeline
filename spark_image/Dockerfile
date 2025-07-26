# Start from the same Bitnami Spark image
FROM bitnami/spark:3.5.1

# Switch to root user to install packages
USER root

# Install pandas using pip
RUN pip install pandas

# Switch back to the default non-root user for security
USER 1001
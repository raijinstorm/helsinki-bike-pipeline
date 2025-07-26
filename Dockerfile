FROM apache/airflow:3.0.2

USER root

RUN apt-get update \
 && apt-get install -y \
      openjdk-17-jdk-headless \  
      && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="${JAVA_HOME}/bin:${PATH}"

# ---- Java is already present ----
# Install a minimal Spark client (3.5.1 w/ Hadoop 3)
ENV SPARK_VERSION=3.5.1
RUN curl -sL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
      | tar -xz -C /opt/ \
 && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

# Make Spark visible to every process
ENV SPARK_HOME=/opt/spark \
    PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

USER airflow

# NOW, set the final PATH for the airflow user, ensuring the local bin is first
ENV PATH="/home/airflow/.local/bin:${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Optional: the PyPI build gives you the pyspark module (needed for find_spark_home)
RUN pip install --no-cache-dir pyspark==${SPARK_VERSION}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt 


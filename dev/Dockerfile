# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG IMAGE_SPARK_VERSION=3.5.6

FROM apache/spark:${IMAGE_SPARK_VERSION}

ARG SPARK_VERSION=3.5.6
ARG SCALA_VERSION=2.12
ARG ICEBERG_SPARK_RUNTIME_VERSION=3.5_2.12
ARG ICEBERG_VERSION=1.10.0
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.753
ARG MAVEN_MIRROR=https://repo.maven.apache.org/maven2

# Install dependencies and download JARs in single layer
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        curl && \
    # Create temporary directory for downloads
    mkdir -p /tmp/jars && \
    cd /tmp/jars && \
    # Download JARs with error handling
    for url in \
        "${MAVEN_MIRROR}/org/apache/spark/spark-connect_${SCALA_VERSION}/${SPARK_VERSION}/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
        "${MAVEN_MIRROR}/org/apache/iceberg/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}-${ICEBERG_VERSION}.jar" \
        "${MAVEN_MIRROR}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
        "${MAVEN_MIRROR}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
        "${MAVEN_MIRROR}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    ; do \
        echo "Downloading: ${url}" && \
        wget --progress=dot:giga --retry-connrefused --waitretry=1 --timeout=60 "${url}" || exit 1; \
    done && \
    # Move JARs to Spark directory
    mv *.jar "${SPARK_HOME}/jars/" && \
    chown spark:spark "${SPARK_HOME}/jars"/*.jar && \
    # Create Spark events directory
    mkdir -p "/home/iceberg/spark-events" && \
    chown spark:spark "/home/iceberg/spark-events" && \
    # Cleanup
    cd / && \
    rm -rf /tmp/jars && \
    apt-get remove -y wget && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Switch back to spark user
USER spark

# Working directory
WORKDIR "${SPARK_HOME}"

# Copy Spark configuration
COPY spark-defaults.conf "${SPARK_HOME}/conf/"

# Create healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:15002/ || exit 1

# Expose Spark Connect port (default is 15002)
EXPOSE 15002

CMD ["/bin/bash", "-c", "${SPARK_HOME}/sbin/start-connect-server.sh && tail -f /dev/null"]

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

ARG BASE_IMAGE_SPARK_VERSION=4.0.1

FROM apache/spark:${BASE_IMAGE_SPARK_VERSION}

# Dependency versions - keep these compatible
# Changing these will invalidate the JAR download cache layer
ARG ICEBERG_VERSION=1.10.1
ARG ICEBERG_SPARK_RUNTIME_VERSION=4.0_2.13
ARG HADOOP_VERSION=3.4.1
ARG AWS_SDK_VERSION=2.24.6
ARG MAVEN_MIRROR=https://repo.maven.apache.org/maven2

USER root
WORKDIR ${SPARK_HOME}

# Install curl and create directories
RUN apt-get update -qq && \
    apt-get install -qq -y --no-install-recommends curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /home/iceberg/spark-events && \
    chown -R spark:spark /home/iceberg

# Download JARs with retry logic (most cacheable - only changes when versions change)
# This is the slowest step, so we do it before copying config files
RUN set -e && \
    cd "${SPARK_HOME}/jars" && \
    for jar_path in \
        "org/apache/iceberg/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}-${ICEBERG_VERSION}.jar" \
        "org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
        "org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
        "software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar"; \
    do \
        jar_name=$(basename "${jar_path}") && \
        curl -fsSL --retry 3 --retry-delay 5 \
             -o "${jar_name}" \
             "${MAVEN_MIRROR}/${jar_path}" && \
        chown spark:spark "${jar_name}"; \
    done

# Copy configuration last (changes more frequently than JARs)
COPY --chown=spark:spark spark-defaults.conf ${SPARK_HOME}/conf/

USER spark
WORKDIR ${SPARK_HOME}

# Start Spark Connect server
CMD ["bash", "-c", "SPARK_NO_DAEMONIZE=true ${SPARK_HOME}/sbin/start-connect-server.sh"]

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

FROM apache/hive:4.0.0

# Dependency versions - changing these invalidates the JAR download layer
ARG HADOOP_VERSION=3.3.6
ARG AWS_SDK_BUNDLE=1.12.753
ARG MAVEN_MIRROR=https://repo1.maven.org/maven2

USER root

# Install curl (separate layer - rarely changes)
RUN apt-get update -qq && \
    apt-get -qq -y install --no-install-recommends curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download JARs with retry logic (slow layer - only changes when versions change)
RUN curl -fsSL --retry 3 --retry-delay 5 \
        -o /opt/hive/lib/hadoop-aws-${HADOOP_VERSION}.jar \
        "${MAVEN_MIRROR}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" && \
    curl -fsSL --retry 3 --retry-delay 5 \
        -o /opt/hive/lib/aws-java-sdk-bundle-${AWS_SDK_BUNDLE}.jar \
        "${MAVEN_MIRROR}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE}.jar"

# Copy configuration last (changes more frequently than JARs)
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml

USER hive

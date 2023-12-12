FROM openjdk:8-jre-slim AS build

RUN apt-get update -qq && apt-get -qq -y install curl

ENV AWSSDK_VERSION=2.20.18
ENV HADOOP_VERSION=3.1.0

RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar -Lo /tmp/aws-java-sdk-bundle-1.11.271.jar
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -Lo /tmp/hadoop-aws-${HADOOP_VERSION}.jar


FROM apache/hive:3.1.3

ENV AWSSDK_VERSION=2.20.18
ENV HADOOP_VERSION=3.1.0

COPY --from=build /tmp/hadoop-aws-${HADOOP_VERSION}.jar /opt/hive/lib/hadoop-aws-${HADOOP_VERSION}.jar
COPY --from=build /tmp/aws-java-sdk-bundle-1.11.271.jar /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
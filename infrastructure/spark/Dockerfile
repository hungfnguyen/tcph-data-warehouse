FROM docker.io/bitnami/spark:3.5.0

USER root

# Install curl để download JARs
RUN apt-get update && apt-get install -y curl && apt-get clean

# Hadoop AWS và S3 dependencies
RUN curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.367.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar

# Delta Lake 2.4.0
RUN curl -L -o /opt/bitnami/spark/jars/delta-core_2.12-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    curl -L -o /opt/bitnami/spark/jars/delta-storage-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

# Copy spark configuration
COPY infrastructure/spark/config/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

# Switch back to spark user
USER 1001
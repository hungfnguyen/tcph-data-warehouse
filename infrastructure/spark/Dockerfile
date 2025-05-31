FROM docker.io/bitnami/spark:3.4.3

USER root

# Install prerequisites
RUN apt-get update && apt-get install -y curl && apt-get clean

# Download required JARs cho S3/MinIO và Delta Lake
RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
  && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar \
  && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
  && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar \
  && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar \
  && mv *.jar /opt/bitnami/spark/jars/

# Copy spark configuration
COPY infrastructure/spark/config/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

# Switch back to spark user
USER 1001
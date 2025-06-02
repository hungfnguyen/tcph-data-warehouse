FROM jupyter/all-spark-notebook:spark-3.5.0

USER root

RUN curl -L -o /usr/local/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /usr/local/spark/jars/aws-java-sdk-bundle-1.12.367.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar && \
    curl -L -o /usr/local/spark/jars/delta-core_2.12-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    curl -L -o /usr/local/spark/jars/delta-storage-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

RUN pip install --no-cache-dir delta-spark==2.4.0

USER jovyan
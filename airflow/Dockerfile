# FROM apache/airflow:2.9.2-python3.10

# USER root

# # Install Java (default-jdk is Java 11 in Debian bookworm)
# RUN apt update && apt install -y default-jdk && \
#     apt clean && rm -rf /var/lib/apt/lists/*

# # Set JAVA_HOME
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# ENV PATH="$JAVA_HOME/bin:$PATH"

# USER airflow

# COPY requirements.txt /requirements.txt
# RUN pip install --upgrade pip
# RUN pip install -r /requirements.txt
FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk ant && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ENV SPARK_HOME=/usr/local/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip && pip install -r /requirements.txt
FROM python:3.11-bullseye as spark-base

ENV SPARK_VERSION=3.5.0
ENV SPARK_MAJOR_VERSION=3.5
# ENV ICEBERG_VERSION=1.4.2

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      openjdk-11-jdk \
      build-essential \
      rsync \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download spark
RUN mkdir -p ${SPARK_HOME} \
 && curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz


FROM spark-base as pyspark

# Install Jupyter and other python deps
COPY requirements.txt .
RUN pip3 install -r requirements.txt

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"

ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

COPY conf/spark-defaults.conf "$SPARK_HOME/conf"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

COPY ./scripts/entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]






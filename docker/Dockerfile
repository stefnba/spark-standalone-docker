FROM python:3.11-bullseye as cluster-base

ENV SPARK_VERSION=3.5.0
ENV SPARK_MAJOR_VERSION=3.5

ENV WORKSPACE = /opt/workspace

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

RUN mkdir -p ${WORKSPACE}

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64/

RUN pip3 install pyspark

FROM cluster-base as spark


ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}

WORKDIR ${SPARK_HOME}

# Download spark
RUN curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 \
 && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz


# Download
# RUN curl https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.25.1/azure-storage-blob-12.25.1.jar -Lo /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar





# Install Jupyter and other python deps
COPY conf/requirements.txt .
RUN pip3 install -r requirements.txt

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

COPY conf/spark-defaults.conf "$SPARK_HOME/conf"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

COPY ./scripts/entrypoint.sh .
ENTRYPOINT ["./entrypoint.sh"]

FROM cluster-base as jupyterlab

RUN pip3 install jupyterlab

EXPOSE 8888

CMD jupyter lab \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root \
    --notebook-dir=${WORKSPACE} \
    --IdentityProvider.token="hallo" \
    --IdentityProvider.password="hallo" \
    --ServerApp.allow_origin="*" \
    --ServerApp.ip="0.0.0.0"





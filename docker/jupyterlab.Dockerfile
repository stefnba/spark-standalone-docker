FROM python:3.11-bullseye 

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

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64/

RUN pip3 install pyspark jupyterlab

EXPOSE 8888

RUN mkdir /notebooks

CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --notebook-dir=/notebooks --NotebookApp.token=
# CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --notebook-dir=/notebooks

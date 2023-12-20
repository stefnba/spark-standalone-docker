# Spark Standalone Cluster with Docker

This is a starter for a containerized standalone Spark Cluster.

## Setup

To submit local python scripts to the cluster, Java needs to be installed on your local machine. For MacOS, the following commands can be used.

```bash
brew install openjdk@11
echo 'export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"' >> ~/.zshrc
sudo ln -sfn /opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-11.jdk
```

After completing the installation, check your local Java version with:

```bash
java --version
```

## Run

### Submit via shell

## References

[Reading Apache Iceberg from Python with PyIceberg](https://medium.com/@tabular/reading-apache-iceberg-from-python-with-pyiceberg-8b8cff36f4f0)
[Setting up a Spark standalone cluster on Docker in layman terms](https://medium.com/@MarinAgli1/setting-up-a-spark-standalone-cluster-on-docker-in-layman-terms-8cbdc9fdd14b)
[Apache Spark Cluster on Docker (ft. a JupyterLab Interface)](https://towardsdatascience.com/apache-spark-cluster-on-docker-ft-a-juyterlab-interface-418383c95445)

### Connect to Azure Data Lake Storage

[Connect Azure ADLS Gen 2](https://subhamkharwal.medium.com/pyspark-connect-azure-adls-gen-2-c4efa5bf016b)
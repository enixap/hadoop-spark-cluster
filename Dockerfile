FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y openjdk-11-jdk openssh-server openssh-client wget curl net-tools vim rsync && apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin
RUN wget --no-check-certificate --tries=3 --timeout=60 -q https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz && tar -xzf hadoop-3.3.4.tar.gz -C /opt/ && mv /opt/hadoop-3.3.4 /opt/hadoop && rm hadoop-3.3.4.tar.gz
RUN wget --no-check-certificate --tries=3 --timeout=60 -q https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz && tar -xzf spark-3.4.1-bin-hadoop3.tgz -C /opt/ && mv /opt/spark-3.4.1-bin-hadoop3 /opt/spark && rm spark-3.4.1-bin-hadoop3.tgz
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys
RUN echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config
COPY config/ /opt/hadoop/etc/hadoop/
COPY spark-config/ /opt/spark/conf/
RUN sed -i 's/\r//' /opt/hadoop/etc/hadoop/hadoop-env.sh
RUN sed -i 's/\r//' /opt/spark/conf/spark-env.sh
RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> /opt/hadoop/etc/hadoop/hadoop-env.sh
RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> /opt/spark/conf/spark-env.sh
COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r//' /entrypoint.sh
RUN chmod +x /entrypoint.sh
EXPOSE 22 9870 8088 7077 8080 4040
ENTRYPOINT ['"/entrypoint.sh"']

#!/bin/bash
service ssh start
sleep 5
if [ "$HOSTNAME" = "master" ]; then
    if [ ! -d "/opt/hadoop/data/namenode" ]; then
        mkdir -p /opt/hadoop/data/namenode
        $HADOOP_HOME/bin/hdfs namenode -format -force
    fi
    $HADOOP_HOME/sbin/start-dfs.sh
    $HADOOP_HOME/sbin/start-yarn.sh
    sleep 10
    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /spark-logs
    $SPARK_HOME/sbin/start-master.sh
    echo '=== Cluster demarre ==='
    echo 'HDFS Web UI  : http://localhost:9870'
    echo 'YARN Web UI  : http://localhost:8088'
    echo 'Spark Web UI : http://localhost:8080'
fi
if [[ "$HOSTNAME" == slave* ]]; then
    sleep 15
    $SPARK_HOME/sbin/start-worker.sh spark://master:7077
fi
tail -f /dev/null

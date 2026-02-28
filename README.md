# TP Hadoop-Spark Cluster

## Description
Cluster Hadoop-Spark avec 1 Master et 4 Slaves sur Docker.

## Structure
- Dockerfile
- docker-compose.yml
- config/          ? Configuration Hadoop
- spark-config/    ? Configuration Spark
- wordcount.py     ? Word Count complet
- wordcount_letter.py ? Word Count par lettre
- queries_rdd.py   ? Requetes avec RDD
- queries_df.py    ? Requetes avec DataFrame
- queries_sql.py   ? Requetes avec Spark SQL

## Lancer le cluster
docker compose up -d

## Technologies
- Hadoop 3.3.4
- Spark 3.4.1
- Docker
- Python / PySpark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('WordCount') \
    .getOrCreate()

sc = spark.sparkContext

# Lire le fichier depuis HDFS
text = sc.textFile('hdfs://master:9000/user/root/input/5000-8.txt')

# Word Count
wordcount = text.flatMap(lambda line: line.split()) \
               .map(lambda word: (word, 1)) \
               .reduceByKey(lambda a, b: a + b) \
               .sortBy(lambda x: x[1], ascending=False)

# Afficher les 20 premiers mots
print('=== TOP 20 MOTS LES PLUS FREQUENTS ===')
for word, count in wordcount.take(20):
    print(f'{word}: {count}')

# Sauvegarder dans HDFS
wordcount.saveAsTextFile('hdfs://master:9000/user/root/output/wordcount')

spark.stop()

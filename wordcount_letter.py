from pyspark import SparkContext
from pyspark.sql import SparkSession

# Lettre a chercher
LETTRE = 's'

spark = SparkSession.builder \
    .appName('WordCount par Lettre') \
    .getOrCreate()

sc = spark.sparkContext

# Lire le fichier depuis HDFS
text = sc.textFile('hdfs://master:9000/user/root/input/5000-8.txt')

# Filtrer les mots commencant par la lettre
wordcount = text.flatMap(lambda line: line.split()) \
               .filter(lambda word: word.lower().startswith(LETTRE.lower())) \
               .map(lambda word: (word, 1)) \
               .reduceByKey(lambda a, b: a + b) \
               .sortBy(lambda x: x[1], ascending=False)

# Afficher les resultats
print(f'=== MOTS COMMENCANT PAR LA LETTRE [{LETTRE.upper()}] ===')
results = wordcount.collect()
print(f'Nombre de mots distincts commencant par [{LETTRE.upper()}] : {len(results)}')
print('---')
for word, count in results:
    print(f'{word}: {count}')

# Sauvegarder dans HDFS
wordcount.saveAsTextFile('hdfs://master:9000/user/root/output/wordcount_letter')

spark.stop()

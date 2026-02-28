from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, stddev, min, max, col, count, round as spark_round
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName('Nombres_Native').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

# ── Load ──────────────────────────────────────────────
raw = sc.textFile('hdfs://master:9000/user/root/input/nombres.txt')
df = (spark.read
      .text('hdfs://master:9000/user/root/input/nombres.txt')
      .withColumnRenamed('value', 'nombre')
      .withColumn('nombre', col('nombre').cast(DoubleType()))
      .filter(col('nombre').isNotNull()))

print('=' * 55)
print('TP2 - FONCTIONS NATIVES SPARK')
print('=' * 55)

# ── 1) Moyenne ────────────────────────────────────────
print('\n--- 1) Moyenne ---')
df.select(spark_round(mean('nombre'), 4).alias('moyenne')).show()

# ── 2) Mediane ────────────────────────────────────────
print('--- 2) Mediane ---')
mediane = df.approxQuantile('nombre', [0.5], 0.0)[0]
print(f'  Mediane = {mediane}')

# ── 3) Ecart-type ─────────────────────────────────────
print('\n--- 3) Ecart-type ---')
df.select(spark_round(stddev('nombre'), 4).alias('ecart_type')).show()

# ── 4) Minimum ────────────────────────────────────────
print('--- 4) Minimum ---')
df.select(min('nombre').alias('minimum')).show()

# ── 5) Maximum ────────────────────────────────────────
print('--- 5) Maximum ---')
df.select(max('nombre').alias('maximum')).show()

# ── 6) Loi de repartition ─────────────────────────────
print('--- 6) Loi de repartition (distribution par tranches) ---')
from pyspark.sql.functions import floor
df.withColumn('tranche', (floor(col('nombre') / 10) * 10).cast('int')) \
  .groupBy('tranche') \
  .agg(count('*').alias('frequence')) \
  .orderBy('tranche') \
  .show()

# Tout en un avec describe()
print('--- Resume statistique complet (describe) ---')
df.describe('nombre').show()

spark.stop()
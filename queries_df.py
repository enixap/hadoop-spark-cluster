from pyspark.sql import SparkSession
from pyspark.sql.functions import month, count, avg, col

spark = SparkSession.builder.appName('CustomerOrder_DataFrame').getOrCreate()

# Charger les donnees
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

cust_schema = StructType([
    StructField('id', StringType(), True),
    StructField('startDate', StringType(), True),
    StructField('nom', StringType(), True)
])

order_schema = StructType([
    StructField('id', StringType(), True),
    StructField('total', FloatType(), True)
])

cust_df = spark.read.csv('hdfs://master:9000/user/root/input/Customer.txt', schema=cust_schema)
order_df = spark.read.csv('hdfs://master:9000/user/root/input/Order.txt', schema=order_schema)

print('='*50)
print('REQUETE 1 - Nombre achats par mois (DataFrame)')
print('='*50)
cust_df.withColumn('mois', month(col('startDate').cast('date'))) \
       .groupBy('mois').count() \
       .orderBy('mois').show()

print('='*50)
print('REQUETE 2 - Clients tries par nom (DataFrame)')
print('='*50)
cust_df.orderBy('nom').show()

print('='*50)
print('REQUETE 3 - Jointure Client-Commande (DataFrame)')
print('='*50)
cust_df.join(order_df, 'id').select('nom', 'total').show()

print('='*50)
print('REQUETE 4 - Montant moyen par client (DataFrame)')
print('='*50)
cust_df.join(order_df, 'id') \
       .groupBy('nom') \
       .agg(avg('total').alias('moyenne')) \
       .orderBy('nom').show()

spark.stop()

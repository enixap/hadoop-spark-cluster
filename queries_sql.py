from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

spark = SparkSession.builder.appName('CustomerOrder_SQL').getOrCreate()

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

# Creer les vues SQL
cust_df.createOrReplaceTempView('customer')
order_df.createOrReplaceTempView('orders')

print('='*50)
print('REQUETE 1 - Nombre achats par mois (SQL)')
print('='*50)
spark.sql('''SELECT MONTH(CAST(startDate AS DATE)) as mois, COUNT(*) as nb_achats FROM customer GROUP BY MONTH(CAST(startDate AS DATE)) ORDER BY mois''').show()

print('='*50)
print('REQUETE 2 - Clients tries par nom (SQL)')
print('='*50)
spark.sql('''SELECT * FROM customer ORDER BY nom''').show()

print('='*50)
print('REQUETE 3 - Jointure Client-Commande (SQL)')
print('='*50)
spark.sql('''SELECT c.nom, o.total FROM customer c, orders o WHERE c.id = o.id''').show()

print('='*50)
print('REQUETE 4 - Montant moyen par client (SQL)')
print('='*50)
spark.sql('''SELECT c.nom, AVG(o.total) as moyenne FROM customer c, orders o WHERE c.id = o.id GROUP BY c.nom ORDER BY c.nom''').show()

spark.stop()

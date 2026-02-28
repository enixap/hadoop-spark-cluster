from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('CustomerOrder_RDD').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

def parse_customer(line):
    parts = line.strip().split(',')
    if len(parts) != 3:
        return None
    cid, date, nom = parts[0].strip(), parts[1].strip(), parts[2].strip()
    # format is DD/MM/YYYY
    if len(date.split('/')) != 3:
        return None
    return (cid, date, nom)

def parse_order(line):
    parts = line.strip().split(',')
    if len(parts) != 2:
        return None
    try:
        return (parts[0].strip(), float(parts[1].strip()))
    except ValueError:
        return None

customer_raw = sc.textFile('hdfs://master:9000/user/root/input/Customer.txt')
order_raw    = sc.textFile('hdfs://master:9000/user/root/input/Order.txt')

customers = customer_raw.map(parse_customer).filter(lambda x: x is not None)
orders    = order_raw.map(parse_order).filter(lambda x: x is not None)

cust_kv  = customers.map(lambda x: (x[0], (x[1], x[2])))
order_kv = orders.map(lambda x: (x[0], x[1]))

print('=' * 55)
print('REQUETE 1 RDD - Nombre de clients par mois')
print('=' * 55)
# date is DD/MM/YYYY so month is index [1] after split('/')
req1 = (customers
        .map(lambda x: (x[1].split('/')[1], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey())
for mois, count in req1.collect():
    print(f'  Mois {mois} : {count} client(s)')

print('=' * 55)
print('REQUETE 2 RDD - Clients tries par nom')
print('=' * 55)
req2 = (customers
        .map(lambda x: (x[2], (x[0], x[1])))
        .sortByKey())
for nom, (cid, date) in req2.collect():
    print(f'  ID: {cid:<5} | Date: {date} | Nom: {nom}')

print('=' * 55)
print('REQUETE 3 RDD - Commandes avec nom client (jointure)')
print('=' * 55)
req3 = cust_kv.join(order_kv)
for cid, ((date, nom), total) in req3.collect():
    print(f'  Nom: {nom:<20} | Total: {total}')

print('=' * 55)
print('REQUETE 4 RDD - Montant moyen par client')
print('=' * 55)
joined_nom_total = (cust_kv
                    .join(order_kv)
                    .map(lambda x: (x[1][0][1], x[1][1])))
req4 = (joined_nom_total
        .mapValues(lambda total: (total, 1))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda x: round(x[0] / x[1], 2))
        .sortByKey())
for nom, avg in req4.collect():
    print(f'  Nom: {nom:<20} | Moyenne: {avg}')

spark.stop()
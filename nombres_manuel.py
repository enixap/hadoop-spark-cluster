from pyspark.sql import SparkSession
import math

spark = SparkSession.builder.appName('Nombres_Manuel').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

raw = (sc.textFile('hdfs://master:9000/user/root/input/nombres.txt')
         .map(lambda line: line.strip())
         .filter(lambda line: line != '')
         .map(lambda line: float(line)))

raw.cache()
n = raw.count()

print('=' * 55)
print('TP2 - SANS FONCTIONS NATIVES SPARK (map/reduce)')
print('=' * 55)
print(f'  Nombre de valeurs : {n}')

# ── 1) Moyenne ────────────────────────────────────────
print('\n--- 1) Moyenne ---')
total = raw.reduce(lambda a, b: a + b)
moyenne = total / n
print(f'  Moyenne = {round(moyenne, 4)}')

# ── 2) Mediane ────────────────────────────────────────
print('\n--- 2) Mediane ---')
sorted_vals = sorted(raw.collect())
if n % 2 == 1:
    mediane = sorted_vals[n // 2]
else:
    mediane = (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
print(f'  Mediane = {mediane}')

# ── 3) Ecart-type ─────────────────────────────────────
print('\n--- 3) Ecart-type ---')
variance = (raw
            .map(lambda x: (x - moyenne) ** 2)
            .reduce(lambda a, b: a + b)) / n
ecart_type = math.sqrt(variance)
print(f'  Ecart-type = {round(ecart_type, 4)}')

# ── 4) Minimum ────────────────────────────────────────
print('\n--- 4) Minimum ---')
minimum = raw.reduce(lambda a, b: a if a < b else b)
print(f'  Minimum = {minimum}')

# ── 5) Maximum ────────────────────────────────────────
print('\n--- 5) Maximum ---')
maximum = raw.reduce(lambda a, b: a if a > b else b)
print(f'  Maximum = {maximum}')

# ── 6) Loi de repartition ─────────────────────────────
print('\n--- 6) Loi de repartition (tranches de 100) ---')

# Taille de tranche adaptee automatiquement selon l etendue
etendue = maximum - minimum
taille_tranche = max(10, int(etendue // 20))  # ~20 tranches max

distribution = (raw
                .map(lambda x: (int(x // taille_tranche) * taille_tranche, 1))
                .reduceByKey(lambda a, b: a + b)
                .sortByKey())

resultats = distribution.collect()
max_freq = max(freq for _, freq in resultats)

print(f'\n  Taille de tranche : {taille_tranche}')
print(f'  {"Tranche":<20} {"Freq":>6}  {"Pct":>6}  Histogramme')
print(f'  {"-" * 65}')
for tranche, freq in resultats:
    pct = (freq / n) * 100
    # Barre proportionnelle sur 30 caracteres max
    barre = '#' * int((freq / max_freq) * 30)
    print(f'  [{int(tranche):>5} -{int(tranche + taille_tranche - 1):>5}]  {freq:>6}  {pct:>5.2f}%  {barre}')

spark.stop()
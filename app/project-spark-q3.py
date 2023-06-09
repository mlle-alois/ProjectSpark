from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from time import sleep

# Initialisation de la SparkSession
spark = SparkSession \
    .builder \
    .appName("spark-ui") \
    .config("spark.executor.memory", "6g") \
    .config("spark.executor.cores", 6) \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = df.withColumn("date", to_date(from_unixtime(unix_timestamp(df.date, "EEE MMM dd HH:mm:ss yyyy Z")))).show(1)

# Lecture du fichier
full_file = "/app/data/full.csv"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(full_file, format="csv") \
    .repartition(16)

# 3. Afficher dans la console les plus gros contributeurs du projet apache/spark sur les 4 dernières années.
# Pas de date en dur dans le code. Pour la conversion vous pouvez vous référer à cette documentation :
# https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

df = df.withColumn("date", to_date(from_unixtime(unix_timestamp(df.date, "EEE MMM dd HH:mm:ss yyyy Z"))))
#Calculer la date d'il y a 4 ans
four_years_ago = current_date() - expr("INTERVAL 4 YEARS")

#Créer un DataFrame contenant cette date et afficher
dfDate = spark.createDataFrame([(1, )], ["id"])  # Un DataFrame avec une seule ligne et une seule colonne
dfDate = dfDate.withColumn("Four_years_ago", lit(four_years_ago))
four_years_ago_new = dfDate.select("Four_years_ago").first()[0]

df.filter((df.repo == "apache/spark") & df.author.isNotNull() & df.date.isNotNull() & (df.date <= four_years_ago_new)) \
    .groupBy("author") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

sleep(1000)

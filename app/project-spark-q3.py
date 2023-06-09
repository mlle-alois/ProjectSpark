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

four_years_ago = year(current_date()) - 4
df.filter((df.repo == "apache/spark") & df.author.isNotNull() & df.date.isNotNull() & (year(df.date) <= four_years_ago)) \
    .groupBy("author") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

sleep(1000)

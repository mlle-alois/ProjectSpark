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
    .option("multiLine", True) \
    .option("escape", '"') \
    .load(full_file, format="csv") \
    .repartition(16)

# 1. Afficher dans la console les 10 projets Github pour lesquels il y a eu le plus de commit.
df.filter(df.repo.isNotNull()) \
    .groupBy("repo") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10)

# 2. Afficher dans la console le plus gros contributeur (la personne qui a fait le plus de commit) du projet apache/spark.
top_contributor = df.filter((df.repo == "apache/spark") & df.author.isNotNull()) \
    .groupBy("author") \
    .count() \
    .orderBy("count", ascending=False) \
    .first()
print("Top contributor: " + top_contributor.author + " (" + str(top_contributor["count"]) + " commits)")

# 3. Afficher dans la console les plus gros contributeurs du projet apache/spark sur les 4 dernières années.
# Pas de date en dur dans le code. Pour la conversion vous pouvez vous référer à cette documentation :
# https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
df_with_date = df.withColumn("date", to_date(from_unixtime(unix_timestamp(df.date, "EEE MMM dd HH:mm:ss yyyy Z"))))

four_years_ago = year(current_date()) - 4
df_with_date.filter((df_with_date.repo == "apache/spark") & df_with_date.author.isNotNull() & \
                        df_with_date.date.isNotNull() & (year(df_with_date.date) <= four_years_ago)) \
    .groupBy("author") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

sleep(2000)

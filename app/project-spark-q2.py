from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from time import sleep

#Initialisation de la SparkSession
spark = SparkSession \
    .builder \
    .appName("spark-ui") \
    .config("spark.executor.memory","6g") \
    .config("spark.executor.cores",6) \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#Lecture du fichier
full_file = "/app/data/full.csv"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(full_file, format="csv") \
    .repartition(16)

# 2. Afficher dans la console le plus gros contributeur (la personne qui a fait le plus de commit) du projet apache/spark.
top_contributor = df.filter((df.repo == "apache/spark") & df.author.isNotNull()) \
                    .groupBy("author") \
                    .count() \
                    .orderBy("count", ascending=False) \
                    .first()
print("Top contributor: " + top_contributor.author + " (" + str(top_contributor["count"]) + " commits)")

sleep(1000)

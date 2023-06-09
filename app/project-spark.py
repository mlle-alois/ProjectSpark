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

df = df.filter(df.repo.isNotNull())
df.groupBy("repo").count().orderBy("count", ascending=False).show(10)


df = df.filter((df.repo == "apache/spark") & df.author.isNotNull())
top_contributor = df.groupBy("author").count().orderBy("count", ascending=False).first().show()

sleep(1000)

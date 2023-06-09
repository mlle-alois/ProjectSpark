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

# 4. Afficher dans la console les 10 mots qui reviennent le plus dans les messages de commit sur l’ensemble des projets.
# Vous prendrez soin d’éliminer de la liste les stopwords pour ne pas les prendre en compte. Vous êtes libre d’utiliser
# votre propre liste de stopwords, vous pouvez sinon trouver des listes ici.(https://www.kaggle.com/rtatman/stopword-lists-for-19-languages?select=englishST.txt)
# Bonus : A la place d’utiliser une liste de stopwords, vous pouvez utiliser le stopWordsRemover de Spark ML.
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.StopWordsRemover.html
# TODO
top_contributor = df.filter((df.repo == "apache/spark") & df.author.isNotNull()) \
                    .groupBy("author") \
                    .count() \
                    .orderBy("count", ascending=False) \
                    .first()
print("Top contributor: " + top_contributor.author + " (" + str(top_contributor["count"]) + " commits)")

sleep(1000)

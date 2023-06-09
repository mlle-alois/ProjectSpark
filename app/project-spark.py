from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from time import sleep
from pyspark.ml.feature import StopWordsRemover

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

df.cache()

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

# 4. Afficher dans la console les 10 mots qui reviennent le plus dans les messages de commit sur l’ensemble des projets.
# Vous prendrez soin d’éliminer de la liste les stopwords pour ne pas les prendre en compte. Vous êtes libre d’utiliser
# votre propre liste de stopwords, vous pouvez sinon trouver des listes ici.(https://www.kaggle.com/rtatman/stopword-lists-for-19-languages?select=englishST.txt)
# Bonus : A la place d’utiliser une liste de stopwords, vous pouvez utiliser le stopWordsRemover de Spark ML.
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.StopWordsRemover.html

# Diviser les messages de commit en mots
df = df.filter(df.message.isNotNull())

## crée un df avec une colonne words avec les mots du message
words = df.withColumn("words", split("message", "\s+"))

# Ignorer les mots vides (stopwords)
stopwords_file = "./app/data/stop_word.txt"
stopwords = spark.read.text(stopwords_file).rdd.map(lambda r: r[0]).collect()
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stopwords)

words = remover.transform(words)

# Compter la fréquence de chaque mot
# on split tous les tableau de mots en un mot par ligne pour pouvoir faire le count
word_counts = words.select(explode("filtered_words").alias("word")) \
    .groupBy("word").count() \
    .orderBy("count", ascending=False)

# Trier par ordre décroissant et sélectionner les 10 premiers mots
top_words = word_counts.orderBy("count", ascending=False).limit(10)

# Afficher les résultats dans la console
top_words.show(truncate=False)

sleep(2000)

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pyspark.ml.feature import StopWordsRemover
from time import sleep

# Initialisation de la SparkSession
spark = SparkSession \
    .builder \
    .appName("spark-ui") \
    .config("spark.executor.memory", "6g") \
    .config("spark.executor.cores", 6) \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Lecture du fichier
full_file = "/app/data/full.csv"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", True) \
    .option("escape", "\'") \
    .load(full_file, format="csv") \
    .repartition(16)

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


sleep(1000)

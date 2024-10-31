from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Initialiser la session Spark
spark = SparkSession.builder.appName("Practice").getOrCreate()

# Lire le fichier CSV
df_pyspark = spark.read.csv("mushrooms.csv", inferSchema=True, header=True)

# Afficher les premières lignes et le schéma
df_pyspark.show(5)
df_pyspark.printSchema()

# Nettoyage des données
categoricalColumns = df_pyspark.columns
for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "_encoded").fit(df_pyspark)
    df_pyspark = stringIndexer.transform(df_pyspark)
    df_pyspark = df_pyspark.withColumn(categoricalCol + "_encoded", df_pyspark[categoricalCol + "_encoded"].cast('int'))

encoded_df = df_pyspark.select([x + "_encoded" for x in categoricalColumns])

# Assemblage des features
featureAssembler = VectorAssembler(inputCols=[x + "_encoded" for x in categoricalColumns[1:]], outputCol="features")
output = featureAssembler.transform(encoded_df)

# Séparer les données en train et test
train, test = output.randomSplit([0.8, 0.2], seed=17)

print("Données Train : ", train.count())
print("Données Test : ", test.count())

# Régression logistique
lr = LogisticRegression(featuresCol='features', labelCol='class_encoded', maxIter=10)
lrModel = lr.fit(train)

# Prédictions
predictions = lrModel.transform(test)

# Évaluation
evaluator = MulticlassClassificationEvaluator(labelCol="class_encoded", predictionCol="prediction")
accuracy = evaluator.evaluate(predictions)
print("TEST AREA UNDER ROC  : ", accuracy)

# Arrêter la session Spark
spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType
from pyspark.ml.feature import StringIndexer
import pandas as pd

# Initialiser la session Spark
spark = SparkSession.builder.appName("Practice").getOrCreate()

# Lire le fichier CSV
df_pyspark = spark.read.csv("mushrooms.csv", inferSchema=True, header=True)

# Afficher les premières lignes et le schéma
df_pyspark.show(5)
df_pyspark.printSchema()


#Clean Data
categoricalColumns  = df_pyspark.columns
l = []
for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol+"_encoded").fit(df_pyspark)
    df_pyspark = stringIndexer.transform(df_pyspark)
    df_pyspark = df_pyspark.withColumn(categoricalCol+"_encoded", df_pyspark[categoricalCol+"_encoded"].cast('int'))
    
encoded_df =  df_pyspark.select([x+"_encoded" for x in categoricalColumns])
encoded_df.show(5)




from pyspark.ml.feature import VectorAssembler

#check des features
#print([x+"_encoded" for x in categoricalColumns[1::]])

featureAssembler = VectorAssembler(inputCols=[x+"_encoded" for x in categoricalColumns[1::]],outputCol="features")
output = featureAssembler.transform(encoded_df)
output.select("features","class_encoded").show(5)



train, test = output.randomSplit([0.8, 0.2], seed=17)

print("données Train : " , train.count())
print("données Test : "  , test.count())

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(featuresCol = 'features', labelCol = 'class_encoded', maxIter=10)
lrModel = lr.fit(train)


predictions = lrModel.transform(test)

predictions.show(5)



from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator()
evaluator.setLabelCol("class_encoded")
evaluator.setPredictionCol("prediction")

print("TEST AREA UNDER ROC  : ", evaluator.evaluate(predictions))
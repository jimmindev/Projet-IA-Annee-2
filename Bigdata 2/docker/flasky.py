from flask import Flask, request, jsonify
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassificationModel

app = Flask(__name__)
CORS(app)

# Initialiser Spark
spark = SparkSession.builder.appName("MushroomPrediction").getOrCreate()

# Charger le modèle entraîné
model = DecisionTreeClassificationModel.load("models/decision_tree_model")

@app.route('/predict', methods=['POST'])
def predict():
    data = request.json

    # Imprimer les données d'entrée
    print("Données d'entrée:", data)

    # Vérifier les types de données
    print("Types de données des entrées :", {key: type(value) for key, value in data.items()})

    # Convertir les données en une liste de tuples
    input_data = [tuple(data[key] for key in [
        "cap-shape_encoded", "cap-surface_encoded", "cap-color_encoded",
        "bruises_encoded", "odor_encoded", "gill-attachment_encoded",
        "gill-spacing_encoded", "gill-size_encoded", "gill-color_encoded",
        "stalk-shape_encoded", "stalk-root_encoded", 
        "stalk-surface-above-ring_encoded", "stalk-surface-below-ring_encoded", 
        "stalk-color-above-ring_encoded", "stalk-color-below-ring_encoded", 
        "veil-type_encoded", "veil-color_encoded", 
        "ring-number_encoded", "ring-type_encoded", 
        "spore-print-color_encoded", "population_encoded", 
        "habitat_encoded"
    ])]

    # Définir les colonnes pour le DataFrame
    columns = [
        "cap-shape_encoded", "cap-surface_encoded", "cap-color_encoded",
        "bruises_encoded", "odor_encoded", "gill-attachment_encoded",
        "gill-spacing_encoded", "gill-size_encoded", "gill-color_encoded",
        "stalk-shape_encoded", "stalk-root_encoded", 
        "stalk-surface-above-ring_encoded", "stalk-surface-below-ring_encoded", 
        "stalk-color-above-ring_encoded", "stalk-color-below-ring_encoded", 
        "veil-type_encoded", "veil-color_encoded", 
        "ring-number_encoded", "ring-type_encoded", 
        "spore-print-color_encoded", "population_encoded", 
        "habitat_encoded"
    ]

    # Créer un DataFrame Spark avec les données de prédiction
    input_df = spark.createDataFrame(input_data, schema=columns)

    # Assemblage des features
    feature_assembler = VectorAssembler(inputCols=columns, outputCol="features")
    feature_data = feature_assembler.transform(input_df)

    # Prédictions
    predictions = model.transform(feature_data)

    # Extraire les résultats
    result = predictions.select("prediction").collect()

    # Retourner la prédiction
    return jsonify(predictions=[int(result[0].prediction)])

# Démarrer le serveur Flask
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

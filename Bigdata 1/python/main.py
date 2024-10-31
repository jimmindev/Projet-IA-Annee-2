from elasticsearch import Elasticsearch
import os

# Charger les variables d'environnement si nécessaire
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD", "changeme")
ES_HOST = os.getenv("ES_HOST", "https://localhost:9200")
CA_CERTS = os.getenv("CA_CERTS", "ca.crt")  # Chemin vers le certificat CA

# Connexion à Elasticsearch avec SSL
es = Elasticsearch(
    ES_HOST,
    basic_auth=("elastic", ELASTIC_PASSWORD),
    verify_certs=True,  # Vérifie les certificats
    ca_certs=CA_CERTS  # Utiliser le certificat CA
)

# Vérifier la connexion
if es.ping():
    print("Connecté à Elasticsearch")
else:
    print("Échec de la connexion")

# Spécifiez l'index dont vous voulez récupérer les données
index_name = "dep25"  # Remplacez par le nom de votre index

def fetch_all_documents(index_name):
    """Récupérer tous les documents d'un index donné."""
    try:
        # Effectuer la recherche
        response = es.search(index=index_name, body={"query": {"match_all": {}}}, size=200)
        hits = response['hits']['hits']
        
        # Afficher les résultats
        print(f"Nombre de documents dans l'index '{index_name}': {len(hits)}")
        for hit in hits:
            print(hit['_source'])  # Affiche le document source

    except Exception as e:
        print(f"Erreur lors de la récupération des données: {e}")

def search_documents(index_name, search_phrase):
    """Recherche des documents contenant une certaine phrase."""
    try:
        # Effectuer la recherche avec match_phrase pour une correspondance exacte
        response = es.search(index=index_name, body={
            "query": {
                "match_phrase": {
                    "voie": search_phrase  # Remplacez 'adresse' par le champ approprié
                }
            },
            "size": 200
        })
        
        hits = response['hits']['hits']
        
        # Afficher les résultats
        print(f"Nombre de documents trouvés pour '{search_phrase}': {len(hits)}")
        for hit in hits:
            print(hit['_source'])  # Affiche le document source

    except Exception as e:
        print(f"Erreur lors de la recherche des données: {e}")

# Récupérer tous les documents
#fetch_all_documents(index_name)

# Rechercher des documents contenant "13 rue du "
search_phrase = "Piémont"
search_documents(index_name, search_phrase)

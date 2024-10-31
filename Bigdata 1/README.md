# Docker-Compose de Elastic + Kibana + logstash


```
git clone https://github.com/jimmindev/Elastic-Search.git
docker-compose up --build -d
docker stats
```

Pour les tests Python [voir](https://github.com/jimmindev/Elastic-Search/blob/main/python/main.py)
(test GET) 

La pipeline Logstash ne fonctionnais pas avec docker a voir avec le tuto Open-class-room

commande CURL : 
```
curl -X GET "https://localhost:9200/dep25/_search" -H "Content-Type: application/json" -u elastic:changeme --cacert ca.crt -k -d '{
  "query": {
    "match_phrase": {
      "voie": "Pi\u00E9mont"
    }
  },
  "size": 200
}'
```


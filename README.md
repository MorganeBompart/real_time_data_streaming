# real_time_data_streaming

# Projet de Traitement de Données en Temps Réel avec Apache Spark et Kafka

Ce projet vise à démontrer la collecte, le traitement et la diffusion de données en temps réel à l'aide d'Apache Spark et Kafka. Il s'appuie sur deux scripts principaux, `kafka_project.py` pour la collecte des données et `spark.py` pour le traitement et la diffusion des données agrégées.

## Contenu du Projet

Le projet se compose des éléments suivants :

- **kafka_project.py**: Ce script est responsable de la collecte des données en temps réel à partir d'une API externe de partage de vélos (Velib), filtrant les données pour ne conserver que celles de deux stations spécifiques, puis les écrivant dans un topic Kafka.

- **spark.py**: Ce script utilise Apache Spark pour traiter les données en temps réel lues à partir du topic Kafka précédent. Il agrège les données par code postal, calculant le nombre total de vélos disponibles, le nombre de vélos électriques et le nombre de vélos mécaniques par code postal. Ensuite, il écrit les statistiques agrégées dans un nouveau topic Kafka.

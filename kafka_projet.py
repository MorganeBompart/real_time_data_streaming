import json
import time
import requests
from kafka import KafkaProducer

# Fonction pour récupérer les données des stations Velib à partir de l'API
def get_velib_data():
    """
    Get velib data from API
    :return: list of station information
    """
    # Effectue une requête GET à l'API Velib pour obtenir les données de disponibilité des stations
    response = requests.get('https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json')
    # Convertit la réponse JSON en un dictionnaire Python
    data = json.loads(response.text)

    # Récupère la liste des stations à partir des données
    stations = data["data"]["stations"]
    # Initialise une liste pour stocker les stations filtrées
    filtered_stations = []

    # Boucle à travers chaque station
    for station in stations:
        # Vérifie si le code de la station est l'un des deux codes spécifiés
        if station['stationCode'] == '16107' or station['stationCode'] == '32017':
            # Ajoute la station filtrée à la liste
            filtered_stations.append(station)

    # Retourne la liste des stations filtrées
    return filtered_stations


# Fonction pour créer un producteur Kafka et envoyer les données Velib
def velib_producer():
    """
    Create a producer to write Velib data in Kafka
    :return:
    """
    # Crée un producteur Kafka pour écrire les données Velib
    producer = KafkaProducer(bootstrap_servers="localhost:9092",
                             value_serializer=lambda x: json.dumps(x).encode('utf-8')
                             )

    # Boucle infinie pour continuellement envoyer des données Velib
    while True:
        # Récupère les données Velib à l'aide de la fonction définie précédemment
        data = get_velib_data()
        # Boucle à travers chaque message de données Velib
        for message in data:
            # Envoie le message à un topic Kafka nommé "velib-projet"
            producer.send("velib-projet", message)
            # Affiche un message pour indiquer que le message a été ajouté avec succès
            print("added:", message)
        # Attend pendant une seconde avant d'envoyer de nouvelles données Velib
        time.sleep(1)


# Point d'entrée du programme
if __name__ == '__main__':
    # Appelle la fonction velib_producer pour démarrer la production de données Velib vers Kafka
    velib_producer()

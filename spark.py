import os

# Définition des variables d'environnement pour Spark et Java
os.environ["SPARK_HOME"] = "/workspaces/Real_Time_Data_Streaming/spark-3.2.3-bin-hadoop2.7"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /workspaces/Real_Time_Data_Streaming/spark-streaming-kafka-0-10-assembly_2.12-3.2.3.jar pyspark-shell'
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

# Importation des modules requis
import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as pysqlf
import pyspark.sql.types as pysqlt
from pyspark.sql.functions import col,expr
import time

# Fonction de traitement des données en batch
def process_batch(df, epoch_id):
    # Sélection unique des codes postaux
    postcodes = df.select("postcode").distinct().collect()
    # Initialisation des listes pour stocker les résultats finaux
    results_postcodes = []
    final_data = []
    
    # Boucle sur les codes postaux
    for row in postcodes:
        results_postcodes.append(row["postcode"])
        
    # Boucle sur chaque code postal pour calculer les statistiques
    for postcode in results_postcodes:
        # Filtrage des données par code postal et suppression des doublons de stationCode
        postcode_df = df.filter(col("postcode") == postcode).dropDuplicates(subset=['stationCode'])
        
        # Initialisation des compteurs pour les statistiques
        nbr_total = 0
        nbr_elec = 0
        nbr_meca = 0

        # Calcul des statistiques pour chaque station
        for row in postcode_df.collect():
            nbr_total += int(row['num_bikes_available'])
            nbr_elec += int(row['num_bikes_available_types'][0]['mechanical'])
            nbr_meca += int(row['num_bikes_available_types'][1]['ebike'])

        # Ajout des statistiques finales dans la liste des résultats finaux
        final_data.append({
            "postcode": postcode,
            "nbr_total": nbr_total,
            "nbr_elec": nbr_elec,
            "nbr_meca": nbr_meca
        })

# Initialisation de la session Spark
spark = (SparkSession
         .builder
         .appName("news")
         .master("local[1]")
         .config("spark.sql.shuffle.partitions", 1)
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3")
         .getOrCreate()
         )

# Lecture des données en temps réel à partir de Kafka
kafka_df = (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "velib-projet")
            .option("startingOffsets", "earliest")
            .load()
            )

# Définition du schéma des données
schema = pysqlt.StructType([
    pysqlt.StructField("stationCode", pysqlt.StringType()),
    pysqlt.StructField("station_id", pysqlt.StringType()),
    pysqlt.StructField("num_bikes_available", pysqlt.IntegerType()),
    pysqlt.StructField("numBikesAvailable", pysqlt.IntegerType()),
    pysqlt.StructField("num_bikes_available_types", pysqlt.ArrayType(pysqlt.MapType(pysqlt.StringType(), pysqlt.IntegerType()))),
    pysqlt.StructField("num_docks_available", pysqlt.IntegerType()),
    pysqlt.StructField("numDocksAvailable", pysqlt.IntegerType()),
    pysqlt.StructField("is_installed", pysqlt.IntegerType()),
    pysqlt.StructField("is_returning", pysqlt.IntegerType()),
    pysqlt.StructField("is_renting", pysqlt.IntegerType()),
    pysqlt.StructField("last_reported", pysqlt.TimestampType())
])

# Lecture des données JSON depuis Kafka et application du schéma
kafka_df = (kafka_df
            .select(pysqlf.from_json(pysqlf.col("value").cast("string"), schema).alias("value"))
            .withColumn("stationCode", pysqlf.col("value.stationCode"))
            .withColumn("station_id", pysqlf.col("value.station_id"))
            .withColumn("stationCode", pysqlf.col("value.stationCode"))
            .withColumn("num_bikes_available", pysqlf.col("value.num_bikes_available"))
            .withColumn("numBikesAvailable", pysqlf.col("value.numBikesAvailable"))
            .withColumn("num_bikes_available_types", pysqlf.col("value.num_bikes_available_types"))
            .withColumn("num_docks_available", pysqlf.col("value.num_docks_available"))
            .withColumn("numDocksAvailable", pysqlf.col("value.numDocksAvailable"))
            .withColumn("is_installed", pysqlf.col("value.is_installed"))
            .withColumn("is_returning", pysqlf.col("value.is_returning"))
            .withColumn("is_renting", pysqlf.col("value.is_renting"))
            .withColumn("last_reported", pysqlf.col("value.last_reported"))
            .withColumn("mechanical ", pysqlf.col("num_bikes_available_types").getItem(0).getItem("mechanical"))
            .withColumn("ebike ", pysqlf.col("num_bikes_available_types").getItem(1).getItem("ebike"))
            )

# Lecture des données des stations à partir d'un fichier CSV
df_station_informations = spark.read.csv("stations_information.csv", header=True)

# Jointure des données Kafka avec les informations des stations
kafka_df = (kafka_df
                .join(df_station_informations, on=["stationCode", "station_id"], how="left")
            )

# Configuration du streaming et traitement en utilisant la fonction process_batch
kafka_df.writeStream \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .start() 

# Boucle infinie pour surveiller les données finales et les écrire dans Kafka
while True:
    if final_data != []:
        final_df = spark.createDataFrame(final_data)
        final_data = []

        col_selections = ["postcode", "nbr_total", "nbr_elec", "nbr_meca"]

        df_out = (final_df
            .withColumn("value", pysqlf.to_json(pysqlf.struct(*col_selections)))
            .select("value")
            )

        df_out.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "velib-projet-final-data") \
            .save()


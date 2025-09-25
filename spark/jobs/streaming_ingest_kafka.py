from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pymongo import MongoClient

# Configuración
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "taxi-trips"

MONGO_URI = "mongodb://mongodb:27017"
MONGO_DB = "taxi_staging"
MONGO_COLLECTION = "trips"

# Esquema de los datos
schema = "trip_id STRING, fare DOUBLE, distance DOUBLE, pickup_latitude DOUBLE, pickup_longitude DOUBLE"

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("TaxiStreamingKafka") \
    .getOrCreate()

# Leer desde Kafka
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parsear JSON
json_df = raw_df.selectExpr("CAST(value AS STRING)")
parsed_df = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Validaciones básicas
clean_df = parsed_df.filter(
    (col("fare") > 0) &
    (col("distance") > 0) &
    (col("pickup_latitude").between(-90, 90)) &
    (col("pickup_longitude").between(-180, 180))
)

# Guardar en MongoDB
def save_to_mongo(df, epoch_id):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    if df.count() > 0:
        db[MONGO_COLLECTION].insert_many([row.asDict() for row in df.collect()])

clean_df.writeStream.foreachBatch(save_to_mongo).start().awaitTermination()

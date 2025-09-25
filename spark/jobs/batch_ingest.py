from pyspark.sql import SparkSession
from pymongo import MongoClient

MONGO_URI = "mongodb://mongodb:27017"
MONGO_DB = "taxi_staging"
MONGO_COLLECTION = "trips"

spark = SparkSession.builder.appName("BatchIngest").getOrCreate()

# Leer CSVs
df = spark.read.csv("/opt/spark-apps/data/csvs/*.csv", header=True, inferSchema=True)

# Validaciones básicas
df_clean = df.filter(df.fare > 0).filter(df.distance > 0)

# Guardar en MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
db[MONGO_COLLECTION].insert_many([row.asDict() for row in df_clean.collect()])

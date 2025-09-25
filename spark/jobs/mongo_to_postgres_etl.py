from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import psycopg2
from pymongo import MongoClient

MONGO_URI = "mongodb://mongodb:27017"
MONGO_DB = "taxi_staging"
MONGO_COLLECTION = "trips"

POSTGRES_HOST = "postgres"
POSTGRES_DB = "taxi_dw"
POSTGRES_USER = "taxi"
POSTGRES_PASSWORD = "taxi123"

spark = SparkSession.builder.appName("MongoToPostgresETL").getOrCreate()

# Leer Mongo
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
data = list(db[MONGO_COLLECTION].find())
df = spark.createDataFrame(data)

# Guardar en PostgreSQL
df.write \
  .format("jdbc") \
  .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}") \
  .option("dbtable", "trips") \
  .option("user", POSTGRES_USER) \
  .option("password", POSTGRES_PASSWORD) \
  .mode("append") \
  .save()

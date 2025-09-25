"# PIDS-interface" 
# Entrar al proyecto
cd /home/jorge/PIDS-interface

# Levantar contenedores
docker compose up -d --build

# Crear topic Kafka
docker exec -it kafka kafka-topics.sh --create --topic taxi-trips --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092

# Ejecutar productor de datos
cd spark/jobs
python3 kafka_producer.py

# Ejecutar Spark Streaming
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/streaming_ingest_kafka.py

# Ejecutar ETL Mongo -> PostgreSQL
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/mongo_to_postgres_etl.py

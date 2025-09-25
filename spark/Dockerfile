FROM bitnami/spark:3.4.1

USER root
RUN apt-get update && apt-get install -y python3-pip python3-dev default-jdk && rm -rf /var/lib/apt/lists/*

# Instalar librerías Python necesarias
COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/spark-apps
COPY jobs/ /opt/spark-apps/

FROM python:3.11-slim

# Встановлюємо Java та curl
RUN apt-get update && apt-get install -y default-jre curl && apt-get clean

# Завантажуємо та розпаковуємо Spark вручну в /opt
RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz -C /opt/ && \
    rm spark-3.5.0-bin-hadoop3.tgz

# Прописуємо змінні оточення (тепер ми точно знаємо, де Spark)
ENV SPARK_HOME=/opt/spark-3.5.0-bin-hadoop3
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

# Встановлюємо лише необхідні Python-ліби
RUN pip install --no-cache-dir kafka-python requests sseclient-py

WORKDIR /app
COPY . .

# Запускаємо через python3
CMD ["python3", "src/spark/processor.py"]
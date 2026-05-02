FROM python:3.11-slim

# ============================================
# Java та Spark
# ============================================

# Встановлюємо Java
RUN apt-get update && \
    apt-get install -y default-jre curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Завантажуємо Spark
RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz -C /opt/ && \
    rm spark-3.5.0-bin-hadoop3.tgz

# Змінні оточення для Spark
ENV SPARK_HOME=/opt/spark-3.5.0-bin-hadoop3
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
ENV PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

# ============================================
# Python Dependencies
# ============================================

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ============================================
# Application Code
# ============================================

WORKDIR /app
COPY . .

# Створюємо директорію для checkpoints
RUN mkdir -p /tmp/spark_checkpoints && chmod 777 /tmp/spark_checkpoints

# ============================================
# Default Command
# ============================================

CMD ["python3", "src/spark/processor.py"]
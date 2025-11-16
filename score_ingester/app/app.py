import os
import pandas as pd
import json
from confluent_kafka import Consumer, Producer, KafkaError
import logging
import os
import psycopg

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic_scoring": os.getenv("KAFKA_SCORING_TOPIC", "scoring")
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_table():
    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "dbdb"),
        user=os.getenv("POSTGRES_USER", "dbuser"),
        password=os.getenv("POSTGRES_PASSWORD", "dbpass")
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS transaction_scores (
                transaction_id UUID PRIMARY KEY,
                score double precision,
                fraud_flag boolean,
                processed_at timestamptz DEFAULT now()
            )
            """)
            conn.commit()
    finally:
        conn.close()

def save_score(transaction_id: str, score: float, fraud_flag: bool):
    """Сохраняет результаты скоринга в PostgreSQL"""
    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "dbdb"),
        user=os.getenv("POSTGRES_USER", "dbuser"),
        password=os.getenv("POSTGRES_PASSWORD", "dbpass")
    )
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
            VALUES (%s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
        """, (transaction_id, score, fraud_flag))
    conn.commit()
    conn.close()

class ScoringConsumerService:
    # Получение результата текущего скора
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': KAFKA_CONFIG["bootstrap_servers"],
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([KAFKA_CONFIG["topic_scoring"]])

    def ingest_results(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                # Десериализация JSON
                data = json.loads(msg.value().decode('utf-8'))[0]
                # сохраняем данные в бд
                save_score(
                    data["transaction_id"],
                    data["score"],
                    bool(data["fraud_flag"])
                )

                logger.info(f"Saved: {data['transaction_id']}")

            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue

if __name__ == "__main__":
    create_table()
    service = ScoringConsumerService()
    service.ingest_results()
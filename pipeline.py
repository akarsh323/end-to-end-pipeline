#  Kafka Weather Data Pipeline

import os
from datetime import datetime
import json
import time
from typing import Dict, Any

from kafka import KafkaProducer, KafkaConsumer
from sqlalchemy import create_engine, Column, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pydantic import BaseSettings
import logging
from logging.handlers import RotatingFileHandler
import schedule
import concurrent.futures

# Configuration management using Pydantic
class Settings(BaseSettings):
    DB1_URI: str
    DB2_URI: str
    TARGET_DB_URI: str
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_TOPIC: str
    LOG_LEVEL: str = "INFO"
    PRODUCER_INTERVAL: int = 60  # seconds

    class Config:
        env_file = ".env"

config = Settings()

# Set up logging
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_file = 'weather_pipeline.log'
file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(log_formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logger = logging.getLogger()
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.setLevel(config.LOG_LEVEL)

# Database setup
Base = declarative_base()

class WeatherData(Base):
    __tablename__ = 'weather_data'
    id = Column(Integer, primary_key=True)
    temperature = Column(Float)
    humidity = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)

# Kafka producer with error handling
class WeatherProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            acks='all'
        )

    def send_data(self, data: Dict[str, Any]):
        future = self.producer.send(config.KAFKA_TOPIC, data)
        try:
            future.get(timeout=10)
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
        else:
            logger.info(f"Successfully sent message to Kafka: {data}")

# Database operations
def get_db_session(db_uri: str):
    engine = create_engine(db_uri)
    Session = sessionmaker(bind=engine)
    return Session()

def fetch_weather_data(db_uri: str) -> Dict[str, float]:
    try:
        with get_db_session(db_uri) as session:
            result = session.execute("SELECT temperature, humidity FROM weather_data ORDER BY timestamp DESC LIMIT 1")
            data = result.fetchone()
            return {"temperature": data[0], "humidity": data[1]}
    except Exception as e:
        logger.error(f"Error fetching data from {db_uri}: {e}")
        return {"temperature": None, "humidity": None}

# Kafka consumer
def consume_weather_data():
    consumer = KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather_consumer_group'
    )

    target_session = get_db_session(config.TARGET_DB_URI)

    for message in consumer:
        data = message.value
        try:
            avg_temp = (data['db1_temperature'] + data['db2_temperature']) / 2
            avg_humidity = (data['db1_humidity'] + data['db2_humidity']) / 2

            if (abs(data['db1_temperature'] - data['db2_temperature']) > 5 or
                abs(data['db1_humidity'] - data['db2_humidity']) > 10):
                
                new_data = WeatherData(temperature=avg_temp, humidity=avg_humidity)
                target_session.add(new_data)
                target_session.commit()
                logger.info(f"Inserted new weather data: temp={avg_temp}, humidity={avg_humidity}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            target_session.rollback()

    target_session.close()

# Producer job
def produce_weather_data():
    producer = WeatherProducer()
    
    db1_data = fetch_weather_data(config.DB1_URI)
    db2_data = fetch_weather_data(config.DB2_URI)

    if all(db1_data.values()) and all(db2_data.values()):
        weather_data = {
            'db1_temperature': db1_data['temperature'],
            'db1_humidity': db1_data['humidity'],
            'db2_temperature': db2_data['temperature'],
            'db2_humidity': db2_data['humidity'],
            'timestamp': datetime.utcnow().isoformat()
        }
        producer.send_data(weather_data)
    else:
        logger.warning("Failed to fetch complete data from one or both databases")

if __name__ == "__main__":
    # Ensure the target database is set up
    target_engine = create_engine(config.TARGET_DB_URI)
    Base.metadata.create_all(target_engine)

    # Schedule the producer job
    schedule.every(config.PRODUCER_INTERVAL).seconds.do(produce_weather_data)

    # Run producer and consumer in separate threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(consume_weather_data)
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Shutting down the pipeline...")
                break
            except Exception as e:
                logger.error(f"An error occurred in the main loop: {e}")

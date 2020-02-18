from kafka import KafkaConsumer
import logging
import time

logger = logging.getLogger(__name__)


def run_consumer_server():
    consumer = KafkaConsumer(
        "org.spark.streaming",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest"
    )

    for message in consumer:
        print(message.value)
        


if __name__ == "__main__":
    run_consumer_server()

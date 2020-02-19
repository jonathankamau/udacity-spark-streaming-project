from pathlib import Path
import producer_server


def run_kafka_server():
    input_file = f"{Path(__file__).parents[0]}/police-department-calls-for-service.json"

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="org.spark.streaming",
        bootstrap_servers="localhost:9092",
        client_id="kafka-server"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()

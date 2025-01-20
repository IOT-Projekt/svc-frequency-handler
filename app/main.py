from kafka_handler import (
    KafkaConfig,
    setup_kafka_consumer,
    setup_kafka_producer,
    send_kafka_message,
)
import json
import logging
import os

# Topic for frequency message
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "frequency")
# Frequency levels in seconds
FREQUENCY_LEVELS = {1: 2, 2: 5, 3: 10, 4: 20, 5: 30, 6: 60, 7: 120}

# set up logging
logging.basicConfig(level=logging.INFO)


def check_change_frequency_temp(old_temp, new_temp, current_level) -> int:
    # Check if the temperature changed or is the same
    if old_temp == new_temp:
        return min((current_level + 1), 7)
    else:
        return max((current_level - 1), 1)


def check_change_frequency_humidity(old_humidity, new_humidity, current_level) -> int:
    # Check if the temperature changed or is the same
    if old_humidity == new_humidity:
        return min((current_level + 1), 7)
    else:
        return max((current_level - 1), 1)


def change_frequency(frequency_producer, level):
    # Create payload with new frequency level and send it to Kafka
    payload = {"frequency": FREQUENCY_LEVELS[level]}
    send_kafka_message(producer=frequency_producer, topic=KAFKA_TOPIC, payload=payload)
    logging.info(f"Changed frequency to {FREQUENCY_LEVELS[level]} seconds.")
    return


def main() -> None:
    # Set up Kafka
    logging.info("Setting up Kafka...")
    kafka_config = KafkaConfig()
    consumer = setup_kafka_consumer(kafka_config, ["temperatures", "humidity"])
    frequency_producer = setup_kafka_producer(kafka_config)
    
    # Initialize variables
    old_temp = None
    old_humidity = None
    current_level = 3

    # Start consuming messages
    logging.info("Consuming messages...")

    for message in consumer:
        logging.info(f"current level: {current_level}")
        
        
        # Get the message value and topic
        topic = message.topic
        message = json.loads(message.value["message"])
        logging.info(f"Received message: {topic} -> {message}")

        # Check if the message is a temperature or humidity message and update the current level
        if topic == "temperatures":
            new_temp = message["temperature_c"]
            current_level = check_change_frequency_temp(
                old_temp, new_temp, current_level
            )
            old_temp = new_temp
        elif topic == "humidity":
            new_humidity = message["humidity"]
            current_level = check_change_frequency_humidity(
                old_humidity, new_humidity, current_level
            )
            old_humidity = new_humidity

        # Change frequency
        change_frequency(frequency_producer, current_level)


if __name__ == "__main__":
    main()

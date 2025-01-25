from kafka_handler import (
    KafkaConfig,
    setup_kafka_consumer,
    setup_kafka_producer,
    send_kafka_message,
)
import json
import logging
import os

# Topic for frequency message from the environment variable
KAFKA_PRODUCEER_TOPIC = os.getenv("KAFKA_PRODUCER_TOPIC", "frequency")
KAFKA_CONSUMER_TOPIC = os.getenv("KAFKA_CONSUMER_TOPIC", "temperatures")

# Dict of frequency levels in seconds
FREQUENCY_LEVELS = {1: 2, 2: 5, 3: 10, 4: 20, 5: 30, 6: 45, 7: 60}

# set up logging
logging.basicConfig(level=logging.INFO)


def check_change_frequency_temp(old_temp, new_temp, current_level) -> int:
    """If the new temperature is the same as the old temperature, increase the frequency level by 1. Otherwise, decrease the frequency level by 1."""
    if old_temp == new_temp:
        return min((current_level + 1), 7) # 7 is the maximum frequency level
    else:
        return max((current_level - 1), 1) # 1 is the minimum frequency level
    

def change_frequency(frequency_producer, level):
    """Create the payload for the new frequency in seconds and send it to Kafka."""
    payload = {"frequency": FREQUENCY_LEVELS[level]}
    send_kafka_message(producer=frequency_producer, topic=KAFKA_PRODUCEER_TOPIC, payload=payload)
    logging.info(f"Changed frequency to {FREQUENCY_LEVELS[level]} seconds.")


def main() -> None:
    # Set up Kafka consumer and producer
    kafka_config = KafkaConfig()
    consumer = setup_kafka_consumer(kafka_config, [KAFKA_CONSUMER_TOPIC])
    frequency_producer = setup_kafka_producer(kafka_config)

    # Initialize variables
    old_temp = None
    current_level = 3

    for message in consumer:
        # Log the received message and current level
        logging.info(f"Received message: {message.topic} -> {message.value}")
        logging.info(f"Current level: {current_level}")

        # Update the new temperature and get the new frequency level
        new_temp = message["temperature_c"]
        current_level = check_change_frequency_temp(old_temp, new_temp, current_level)
        old_temp = new_temp

        # Send the new frequency to Kafka
        change_frequency(frequency_producer, current_level)
        

if __name__ == "__main__":
    main()

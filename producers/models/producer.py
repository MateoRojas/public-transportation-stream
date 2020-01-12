"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import (
    AvroProducer,
    CachedSchemaRegistryClient
)

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    #TODO: This could be on a config object or somthing!
    #TODO: Change this when running from within docker
    BROKER_URL = "PLAINTEXT://localhost:9092"
    SCHEMA_REGISTRY_URL = "http://localhost:8081"

    #TODO: Check if this is Thread-safe!
    admin_client = AdminClient({"bootstrap.servers": BROKER_URL})

    def __init__(
        self,
        topic_name,
        #TODO: All this could probably be a Topic class
        key_schema,
        value_schema,
        number_partitions=1,
        number_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.number_partitions = number_partitions
        self.number_replicas = number_replicas

        schema_registry = CachedSchemaRegistryClient(Producer.SCHEMA_REGISTRY_URL)

        # If the topic does not already exist, try to create it
        # TODO: I completely dislike this way of looking for existing topics. Cause it resets every time the app is restarted
        # if self.topic_name not in Producer.existing_topics:
        #     self.create_topic()
        #     Producer.existing_topics.add(self.topic_name)
        if (self.topic_does_not_exist(topic_name)):
            logger.info(f'Topic "{topic_name}" does not exist... creating it')
            self.create_topic(topic_name, number_partitions, number_replicas)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            {
                'bootstrap.servers': Producer.BROKER_URL
                #TODO: find if other configurations are needed
            },
            schema_registry=schema_registry,
            default_key_schema=key_schema,
            default_value_schema=value_schema
        )

    def topic_does_not_exist(self, topic_name):
        """Checks if a topic doesn't exists already"""
        return topic_name not in Producer.admin_client.list_topics().topics.keys()

    def create_topic(self, topic_name, number_partitions, number_replicas):
        """Creates the producer topic if it does not already exist"""
        #TODO: check the configs we want for the a topic!
        futureMap = Producer.admin_client.create_topics([NewTopic(topic_name, number_partitions, number_replicas, config = {})])

        # Wait for each operation to finish.
        for topic, future in futureMap.items():
            try:
                future.result()  # The result itself is None
                logger.info(f"Topic {topic} created")
            except Exception as e:
                logger.error(f"Failed to create topic {topic}: {e}")


    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #TODO: see if any other clean up code is needed
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

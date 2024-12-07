from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)


# Визначення нового топіку
my_name = "viktoriia_streaming"
topic_names= [f'{my_name}_building_sensors', f'{my_name}_alerts']#, f'{my_name}_humidity_alerts'

building_sensors = NewTopic(name=topic_names[0], num_partitions=2, replication_factor=1)
temperature_alerts = NewTopic(name=topic_names[1], num_partitions=1, replication_factor=1)
#humidity_alerts = NewTopic(name=topic_names[2], num_partitions=1, replication_factor=1)

# Створення нового топіку
try:
    admin_client.create_topics(new_topics=[building_sensors, temperature_alerts], validate_only=False)
    print(f"Topics {topic_names} created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список існуючих топіків 
[print(topic) for topic in admin_client.list_topics() if my_name in topic]

# Закриття зв'язку з клієнтом
admin_client.close()


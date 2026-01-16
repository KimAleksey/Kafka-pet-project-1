import os
import logging

from pathlib import Path
from dotenv import load_dotenv

from confluent_kafka.admin import AdminClient, NewTopic

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

def get_bootstrap_server() -> str:
    """
    Получаем bootstrap_server.

    :return: URL: str.
    """
    # Получаем секреты
    env_path = Path(__file__).resolve().parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        raise FileNotFoundError(f"Environment file {env_path} not found")
    # URL schema registry
    bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    logging.info(f"Bootstrap server: {bootstrap_server}")
    return bootstrap_server


def get_users_coordinates_topic() -> str:
    """
    Получаем users_coordinates topic.

    :return: URL: str.
    """
    # Получаем секреты
    env_path = Path(__file__).resolve().parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        raise FileNotFoundError(f"Environment file {env_path} not found")
    # URL schema registry
    users_coordinates_topic = os.getenv("KAFKA_USERS_COORDINATES_TOPIC")
    logging.info(f"User coordinates topic: {users_coordinates_topic}")
    return users_coordinates_topic


def create_topic(
    topic_name: str,
    num_partitions: int = 1,
    replication_factor: int = 1,
    min_insync_replicas: int = 1,
    bootstrap_servers: str = None
):
    """
    Создаёт Kafka-топик с заданными параметрами.

    :param topic_name: имя топика
    :param num_partitions: количество партиций
    :param replication_factor: фактор репликации
    :param min_insync_replicas: минимальное количество реплик, которые должны быть in-sync
    :param bootstrap_servers: адрес брокеров Kafka
    """
    if bootstrap_servers is None:
        raise ValueError("Необходимо указать bootstrap_servers")

    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    # Создаем объект топика
    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        config={
            "min.insync.replicas": str(min_insync_replicas)
        }
    )

    # Отправляем запрос на создание топика
    fs = admin_client.create_topics([new_topic])

    # Ждём результат
    for topic, f in fs.items():
        try:
            f.result()  # блокируемся до завершения операции
            logging.info(f"Топик '{topic}' успешно создан")
        except Exception as e:
            logging.error(f"Не удалось создать топик '{topic}': {e}")


def create_users_coordinates_topic():
    # Получаем хост брокера
    bootstrap_serv = get_bootstrap_server()
    # Создаем топик для users_coordinates
    create_topic(
        topic_name="users_coordinates",
        num_partitions=6,
        replication_factor=2,
        min_insync_replicas=1,
        bootstrap_servers=bootstrap_serv
    )
    # Проверка, что топик есть
    get_users_coordinates_topic()


if __name__ == "__main__":
    create_users_coordinates_topic()
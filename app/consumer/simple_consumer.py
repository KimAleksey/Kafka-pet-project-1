import json
import logging

from confluent_kafka import DeserializingConsumer
from confluent_kafka import KafkaError, TopicPartition
from confluent_kafka.error import ValueDeserializationError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer, SerializationContext

from app.utils.schema_registry import USERS_COORDINATES_JSON_SCHEMA, get_schema_registry_url
from app.utils.kafka import get_bootstrap_server, get_users_coordinates_topic

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def dict_from_dict(obj, ctx: SerializationContext):
    """
    Функция преобразования данных после десериализации JSON.

    Используется JSONDeserializer'ом для преобразования
    Python-словаря, полученного после:
      1) чтения сообщения из Kafka
      2) валидации данных по JSON Schema из Schema Registry

    В данном случае дополнительное преобразование не требуется,
    поэтому функция просто возвращает исходный dict.

    :param obj: Данные сообщения в виде dict после валидации схемы
    :param ctx: Контекст сериализации (topic, field, headers и т.д.)
    :return: Python dict с данными сообщения
    """
    return obj


def log_skiped_message(e: ValueDeserializationError) -> None:
    """
    Логируем сообщение, которое пропускаем, так как оно не соответствует Schema Registry.

    :param e: Объект с ошибкой.
    :return: None.
    """
    kafka_msg = e.kafka_message  # объект сообщения, на котором ошибка
    topic = kafka_msg.topic()
    partition = kafka_msg.partition()
    offset = kafka_msg.offset()

    # Сырой payload
    raw_bytes = kafka_msg.value()
    try:
        raw_str = raw_bytes.decode("utf-8")
    except Exception:
        raw_str = str(raw_bytes)
    logging.warning(
        f"Пропущено сообщение в "
        f"topic={topic}, partition={partition}, offset={offset}. "
        f"Сообщение: {raw_str}. Причина: {e}"
    )


def consume(auto_commit: bool = True, reset_offsets: bool = False):
    # Получаем URL Schema Registry из конфигурации
    schema_registry_url = get_schema_registry_url()
    # Получаем адрес Kafka брокеров
    bootstrap_server = get_bootstrap_server()
    # Получаем имя топика
    users_coordinates_topic = get_users_coordinates_topic()
    # Создаём клиента для подключения к Schema Registry
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
    # Получаем JSON Schema в виде строки
    json_schema_str = json.dumps(USERS_COORDINATES_JSON_SCHEMA)
    # Десериализируем сообщение в соответствии со схемой.
    json_deserializer = JSONDeserializer(
        schema_str=json_schema_str,
        from_dict=dict_from_dict,
        schema_registry_client=schema_registry_client,
    )
    # Конфигурация Consumer.
    python_consumer_conf = {
        # Адрес Kafka брокеров
        "bootstrap.servers": bootstrap_server,

        # Consumer группа.
        "group.id": "simple_consumer.py",

        # Kafka начинает читать сначала, если offset не задан и не найден для Consumer.
        "auto.offset.reset": "earliest",

        # Kafka сама отметит, что сообщения до этого смещения прочитаны.
        "enable.auto.commit": auto_commit,

        # Десериализатор ключа.
        "key.deserializer": StringDeserializer("utf-8"),

        # Десериализатор значения.
        "value.deserializer": json_deserializer,
    }

    # Создание Consumer с ранее заданным десериализотором с проверкой схемы.
    consumer = DeserializingConsumer(python_consumer_conf)

    if not reset_offsets:
        # Получаем список партиций
        partitions = consumer.list_topics(users_coordinates_topic).topics[users_coordinates_topic].partitions
        # Вручную задаём offset для каждой партиции.
        consumer.assign([TopicPartition(users_coordinates_topic, p, 0) for p in partitions])
        logging.info("Offsets для всех партиций установлены на 0.")
    else:
        # Подписываемся на topic, Kafka сама определяет с какого offset начать.
        consumer.subscribe([users_coordinates_topic])

    try:
        # Запускаем бесконечный цикл для постоянного чтения сообщений из Kafka
        while True:
            # Пытаемся получить сообщение из consumer с тайм-аутом 1.0 секунду
            try:
                msg = consumer.poll(1.0)
            # Если возникает ошибка с несоответствием схемы.
            except ValueDeserializationError as e:
                log_skiped_message(e)
                continue
            # Если сообщений нет, poll вернул None → просто продолжаем цикл
            if msg is None:
                continue
            # Проверяем, есть ли ошибка при получении сообщения
            if msg.error():
                # Если достигнут конец партиции (KafkaError == end of partition)
                if msg.error().code() == KafkaError:
                    logging.error("Reached end of partition.")
                else:
                    # Любая другая ошибка при чтении сообщения
                    logging.error(f"Error: {msg.error()}")
                # Пропускаем это сообщение
                continue
            # Если сообщение получено без ошибок
            try:
                # Десериализация значения сообщения (JSONDeserializer сразу возвращает Python dict)
                value = msg.value()
                # Логируем успешно полученное сообщение
                logging.info(f"Received message: {value}")
                # Бизнес логика тут
                pass
                # Ручной коммит текущего сообщения
                if not auto_commit:
                    consumer.commit(msg)
            except Exception as e:
                # Ловим ошибки несоответствия сообщения схеме (например, битый JSON или некорректные поля)
                # Логируем предупреждение и пропускаем некорректное сообщение
                logging.warning(f"Skipping invalid message: {msg.value()}. Reason: {e}")
                continue
    except KeyboardInterrupt:
        logging.info("Stopping consumer")
    finally:
        # Закрываем consumer и освобождаем ресурсы (важно при работе с Kafka)
        consumer.close()

def main():
    # Запуск чтения с автокоммитом offset.
    # consume(auto_commit=True)
    # Запуск чтения с ручным коммитом offset.
    # consume(auto_commit=False)
    # Запуск чтения с ручным коммитом offset, сначала.
    # consume(auto_commit=False, reset_offsets=False)
    # Запуск чтения с ручным коммитом offset, с последнего места.
    consume(auto_commit=False, reset_offsets=True)


if __name__ == "__main__":
    main()
import json
import time

from app.utils.schema_registry import USERS_COORDINATES_JSON_SCHEMA, get_schema_registry_url
from app.utils.kafka import get_bootstrap_server, get_users_coordinates_topic
from app.utils.generator import generate_user_coordinates

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")


def produce(with_errors: bool = False) -> None:
    """
    Отправляет сообщение в Kafka в топик users_coordinates.

    :param with_errors: Если True, то имитируем отправку невалидного сообщения.
    :return: None.
    """
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

    # Создаём сериализатор, который:
    # 1. валидирует данные по схеме
    # 2. регистрирует схему в Schema Registry
    # 3. сериализует объект в байты перед отправкой в Kafka
    json_serializer = JSONSerializer(
        json_schema_str,
        schema_registry_client
    )

    # Конфигурация продюсера Kafka
    producer_conf = {
        # Адрес Kafka брокеров
        "bootstrap.servers": bootstrap_server,

        # Сериализатор ключа сообщений — здесь используется IntegerSerializer, так как ключ "id" целочисленный
        "key.serializer": StringSerializer("utf_8"),

        # Сериализатор значения сообщений — JSONSerializer для отправки словаря Python как JSON
        "value.serializer": json_serializer,

        # Количество подтверждений от брокеров для записи сообщения (все)
        "acks": -1,

        # Размер пакета в 1000 байт. Когда накопится нужный объем - запишется в Kafka
        "batch.size": 1000,

        # Ждать до 10 секунды, чтобы собрать больше сообщений. Сработает либо по размеру, либо по времени.
        "linger.ms": 10000,

        # Включает идемпотентность, чтобы не было дубликатов сообщений при повторной отправке.
        "enable.idempotence": True,

        # Количество попыток повторной отправки сообщений при сбоях
        "retries": 100,

        # Максимальное количество сообщений, которые могут быть отправлены одновременно через одно соединение.
        # В сочетании с идемпотентностью нужно держать ≤ 5, иначе возможны перестановки сообщений.
        "max.in.flight.requests.per.connection": 1,
    }

    # Создаём продюсера с указанной конфигурацией
    producer = SerializingProducer(producer_conf)

    try:
        while True:
            # Генерация данных пользователя (словарь с ключами 'id', 'longitude', 'latitude')
            data = generate_user_coordinates()

            # Имитируем ошибку в формате отправляемых данных
            if with_errors:
                # Удаляем поле "latitude", чтобы структура данных не соответствовала JSON-схеме
                data.pop("latitude")

            # Отправка сообщения в Kafka
            producer.produce(
                topic=users_coordinates_topic,  # Топик, куда отправляем сообщение
                key=str(data["id"]),            # Ключ сообщения (используется для партиционирования)
                value=data,                     # Значение сообщения (словарь с координатами)
                on_delivery=delivery_report,    # Колбэк, который вызывается после доставки (или ошибки)
            )

            # Обработать доставленные сообщения
            producer.poll(0)

            # Имитация отправки раз в секунду
            time.sleep(10)
    except KeyboardInterrupt:
        # Принудительно отправляем все накопленные сообщения (flush ждёт завершения всех операций)
        producer.flush()


def main():
    # Отправка сообщения без ошибки
    produce(with_errors=False)

    # Имитация ошибки в отправляемых данных
    # produce(with_errors=True)

if __name__ == "__main__":
    main()

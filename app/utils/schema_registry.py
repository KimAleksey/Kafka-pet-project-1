import os
import json
import logging
import requests

from pathlib import Path
from dotenv import load_dotenv
from typing import Any

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# Схема для координат пользователей
USERS_COORDINATES_JSON_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "UserCoordinates",
    "type": "object",
    "properties": {
      "id": {"type": "integer"},
      "longitude": {"type": "number"},
      "latitude": {"type": "number"}
    },
    "required": ["id", "longitude", "latitude"]
}


def get_users_coordinates_subject() -> str:
    """
    Получаем название схемы для данных о координатах пользователей.

    :return: Schema name: str.
    """
    # Получаем секреты
    env_path = Path(__file__).resolve().parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        raise FileNotFoundError(f"Environment file {env_path} not found")
    # Schema name
    schema_registry_url = os.getenv("KAFKA_USERS_COORDINATES_SUBJECT")
    logging.info(f"Schema name: {schema_registry_url}")
    return schema_registry_url


def get_schema_registry_url() -> str:
    """
    Получаем URL для Schema registry.

    :return: URL: str.
    """
    # Получаем секреты
    env_path = Path(__file__).resolve().parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        raise FileNotFoundError(f"Environment file {env_path} not found")
    # URL schema registry
    schema_registry_url = os.getenv("KAFKA_SCHEMA_REGISTER_URL")
    logging.info(f"Schema registry url: {schema_registry_url}")
    return schema_registry_url


def create_user_coordinates_schema(
        url: str | None,
        json_schema: dict[Any, Any] | None,
        subject: str | None = None
) -> None:
    """
    Создаем схему для users_coordinates.

    :return: None.
    """
    if json_schema is None:
        json_schema = USERS_COORDINATES_JSON_SCHEMA.deepcopy()

    if subject is None:
        subject = get_users_coordinates_subject()

    payload = {
        "schemaType": "JSON",
        "schema": json.dumps(json_schema)
    }

    response = requests.post(
        f"{url}/subjects/{subject}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        json=payload
    )

    if response.ok:
        logging.info(f"✅ Схема зарегистрирована. Schema ID: {response.json()["id"]}")
    else:
        logging.info(f"❌ Схема не зарегистрирована. {response.status_code} {response.text}")


def get_user_coordinates_schema(url: str, subject: str | None = None) -> None:
    """
    Выводим в консоль информацию о схеме

    :return: None.
    """
    if subject is None:
        subject = get_users_coordinates_subject()
    # Получаем URL
    user_coordinates_schema_url = url + f"/subjects/{subject}/versions/1"
    # Запрашиваем данные схемы
    response = requests.get(url=user_coordinates_schema_url)

    if response.ok:
        logging.info(f"✅ Схема {subject} получена. Schema: {response.json()['schema']}")
    else:
        logging.info(f"❌ Схема {subject} не получена. {response.status_code} {response.text}")


def create_users_coordinates_schema():
    base_url = get_schema_registry_url()
    schema_subject = get_users_coordinates_subject()
    create_user_coordinates_schema(url=base_url, json_schema=USERS_COORDINATES_JSON_SCHEMA, subject=schema_subject)
    get_user_coordinates_schema(base_url, subject=schema_subject)


if __name__ == "__main__":
    create_users_coordinates_schema()
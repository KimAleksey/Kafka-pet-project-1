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

# Название схемы.
USERS_COORDINATES_SUBJECT = "users-coordinates-values"


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
        subject: str | None = USERS_COORDINATES_SUBJECT
) -> None:
    """
    Создаем схему для users_coordinates.

    :return: None.
    """
    if json_schema is None:
        json_schema = USERS_COORDINATES_JSON_SCHEMA.deepcopy()

    payload = {
        "schemaType": "JSON",
        "schema": json.dumps(json_schema)
    }

    response = requests.post(
        f"{url}/subjects/{USERS_COORDINATES_SUBJECT}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        json=payload
    )

    if response.ok:
        logging.info(f"✅ Схема зарегистрирована. Schema ID: {response.json()["id"]}")
    else:
        logging.info(f"❌ Схема не зарегистрирована. {response.status_code} {response.text}")


def get_user_coordinates_schema(url: str, subject: str | None = USERS_COORDINATES_SUBJECT) -> None:
    """
    Выводим в консоль информацию о схеме

    :return: None.
    """
    # Получаем URL
    user_coordinates_schema_url = url + f"/subjects/{USERS_COORDINATES_SUBJECT}/versions/1"
    # Запрашиваем данные схемы
    response = requests.get(url=user_coordinates_schema_url)

    if response.ok:
        logging.info(f"✅ Схема {USERS_COORDINATES_SUBJECT} получена. Schema: {response.json()['schema']}")
    else:
        logging.info(f"❌ Схема {USERS_COORDINATES_SUBJECT} не получена. {response.status_code} {response.text}")


if __name__ == "__main__":
    base_url = get_schema_registry_url()
    create_user_coordinates_schema(url=base_url, json_schema=USERS_COORDINATES_JSON_SCHEMA)
    get_user_coordinates_schema(base_url)
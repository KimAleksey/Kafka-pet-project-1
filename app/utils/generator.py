import random
import uuid
import faker
import logging

from typing import Any

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def generate_user_coordinates() -> dict[str, Any]:
    """
    Возвращает координаты пользователя: id, широта, долгота.

    :return: Dict - {"id": uuid, longitude: float, latitude: float}.
    """
    fake = faker.Faker(locale='ru_RU')
    user_coordinates = {
        "id": random.randint(1,100),
        "longitude": fake.longitude(),
        "latitude": fake.latitude(),
    }
    # Логирование
    logging.info(f"""
    Координаты пользователя {user_coordinates['id']}:
      longitude = {user_coordinates['longitude']}
      latitude  = {user_coordinates['latitude']}
    """.strip())

    return user_coordinates



if __name__ == "__main__":
    generate_user_coordinates()
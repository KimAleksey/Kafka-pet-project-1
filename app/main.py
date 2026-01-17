import multiprocessing
import subprocess

from app.utils.kafka import create_users_coordinates_topic
from app.utils.schema_registry import create_users_coordinates_schema


def run_script(script_path):
    subprocess.run(["python", script_path])


def main():
    # Создание Topic.
    create_users_coordinates_topic()
    # Создание Schema.
    create_users_coordinates_schema()

    # Producer
    script1 = "./producer/simple_producer.py"
    # Consumer
    script2 = "./consumer/simple_consumer.py"

    # Создаём процессы
    p1 = multiprocessing.Process(target=run_script, args=(script1,))
    p2 = multiprocessing.Process(target=run_script, args=(script2,))

    # Запускаем процессы
    p1.start()
    p2.start()


if __name__ == "__main__":
    main()
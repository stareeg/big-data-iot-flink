"""Единый конфиг проекта. Значения по умолчанию подходят для локального
docker-compose, любое можно переопределить переменной окружения, не правя код."""
import logging
import os
from pathlib import Path

# Корень проекта, от него считаем путь к jar
ROOT = Path(__file__).resolve().parent


def _env_int(name, default):
    # Числовой параметр из окружения с понятной ошибкой. Иначе на import config
    # любой опечатанный PG_PORT валил бы всё сырым ValueError без имени переменной
    raw = os.getenv(name, default)
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"переменная окружения {name} должна быть целым числом, получено {raw!r}")


# Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
INPUT_TOPIC = os.getenv("IOT_INPUT_TOPIC", "iot_events")
OUTPUT_TOPIC = os.getenv("IOT_OUTPUT_TOPIC", "iot_aggregates")

# PostgreSQL. Контейнер опубликован на 5433, потому что 5432 часто занят
# локальной установкой PostgreSQL (см. docker-compose и logs/SUMMARY.md)
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = _env_int("PG_PORT", "5433")
PG_DB = os.getenv("PG_DB", "iot")
PG_USER = os.getenv("PG_USER", "iot")
PG_PASSWORD = os.getenv("PG_PASSWORD", "iot")

# JDBC-адрес того же PostgreSQL, понадобится Flink-коннектору на шаге 4
PG_JDBC_URL = os.getenv("PG_JDBC_URL", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")

# Папка с jar-коннекторами Flink, наполняется scripts/fetch_jars.ps1.
# resolve, чтобы as_uri ниже работал даже при относительном переопределении
JARS_DIR = Path(os.getenv("FLINK_JARS_DIR", str(ROOT / "jars"))).resolve()

# Параметры окна, пригодятся на шагах 5 и 7
WINDOW_MINUTES = _env_int("IOT_WINDOW_MINUTES", "1")

# Запас watermark на опоздание событий в секундах (шаг 5). Поток с генератора
# монотонный, небольшой запас держим на случай задержек в брокере
WATERMARK_DELAY_SECONDS = _env_int("IOT_WATERMARK_DELAY_SECONDS", "2")

# Имена таблиц и представлений Flink в одном месте, чтобы не плодить строки по модулям
EVENTS_VIEW = os.getenv("IOT_EVENTS_VIEW", "iot_events")
DEVICE_TYPES_TABLE = os.getenv("IOT_DEVICE_TYPES_TABLE", "device_types")
ENRICHED_VIEW = os.getenv("IOT_ENRICHED_VIEW", "enriched_events")
AGGREGATES_VIEW = os.getenv("IOT_AGGREGATES_VIEW", "aggregates")
# Таблица-приёмник Flink, пишет в топик OUTPUT_TOPIC (шаг 8)
AGGREGATES_SINK_TABLE = os.getenv("IOT_AGGREGATES_SINK", "iot_aggregates_sink")

# Формат базового логирования для точек входа
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def configure_logging():
    # Единая настройка логов для main и генератора
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
    # PyFlink при импорте опускает уровень корневого логгера, и сообщения о сборке
    # графа пропадают. Наши логгеры держим на INFO явно, чтобы вывод не зависел от этого
    for name in ("main", "iot_producer", "pipeline"):
        logging.getLogger(name).setLevel(logging.INFO)


def pg_connect_kwargs():
    # Аргументы для psycopg2.connect
    return dict(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)


def jar_urls():
    # Flink ждёт пути в виде file URL, на Windows as_uri даёт нужный формат
    return [p.as_uri() for p in sorted(JARS_DIR.glob("*.jar"))]

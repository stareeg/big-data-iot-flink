"""Генератор событий IoT. Раз в секунду публикует JSON в Kafka.
Запускается отдельно (python iot_producer.py) или из main.py с флагом --with-producer."""
import json
import logging
import random
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from config import KAFKA_BROKER, INPUT_TOPIC, configure_logging

log = logging.getLogger("iot_producer")

# Период публикации, раз в секунду по ТЗ
PERIOD_SECONDS = 1.0

# Как часто печатать сводку по сгенерированным событиям
STATS_EVERY = 10

# Диапазон id типов устройств. Должен совпадать с dml.sql (1..5),
# иначе join со справочником на шаге 6 отбросит событие
DEVICE_TYPE_IDS = (1, 2, 3, 4, 5)

# Реалистичные диапазоны микроклимата из домена диплома
TEMPERATURE_RANGE = (18.0, 28.0)
HUMIDITY_RANGE = (30.0, 70.0)


def new_stats():
    # Накопитель статистики, чтобы печатать сводку, а не только каждое событие
    return {"total": 0, "by_type": {i: 0 for i in DEVICE_TYPE_IDS}, "sum_temp": 0.0, "sum_hum": 0.0}


def update_stats(stats, event):
    # Учитываем одно отправленное событие
    stats["total"] += 1
    stats["by_type"][event["device_type_id"]] += 1
    stats["sum_temp"] += event["temperature"]
    stats["sum_hum"] += event["humidity"]


def summary_line(stats, elapsed):
    # Понятная сводка для терминала, без двоеточий и спецзнаков
    total = stats["total"]
    avg_temp = stats["sum_temp"] / total if total else 0.0
    avg_hum = stats["sum_hum"] / total if total else 0.0
    by_type = " ".join(f"id{i} {stats['by_type'][i]}" for i in DEVICE_TYPE_IDS)
    return (f"за {elapsed:.0f} секунд отправил {total} событий, "
            f"темп около {avg_temp:.1f}, влажность около {avg_hum:.1f}, "
            f"по типам {by_type}")


def build_event():
    # event_time берём в момент отправки, строкой ISO с миллисекундами.
    # Время наивное локальное, поэтому граница окна на шаге 7 будет в местном времени
    event_time = datetime.now().isoformat(timespec="milliseconds")
    return {
        "device_type_id": random.choice(DEVICE_TYPE_IDS),
        "event_time": event_time,
        "temperature": round(random.uniform(*TEMPERATURE_RANGE), 1),
        "humidity": round(random.uniform(*HUMIDITY_RANGE), 1),
    }


def on_send_error(exc):
    # Колбэк ошибки работает в потоке отправителя, исключение отсюда не доходит
    # до основного цикла, поэтому просто логируем. Фатальные ошибки ловит flush ниже
    log.error("ошибка отправки в Kafka %s", exc)


def run():
    configure_logging()
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            # Сериализуем dict в JSON и далее в байты utf-8
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except NoBrokersAvailable:
        log.error("Kafka недоступна на %s, поднят ли docker compose up -d", KAFKA_BROKER)
        raise SystemExit(1)

    log.info("генератор запущен, шлю JSON раз в секунду в топик %s на брокер %s", INPUT_TOPIC, KAFKA_BROKER)
    start = time.monotonic()
    stats = new_stats()
    try:
        while True:
            event = build_event()
            try:
                producer.send(INPUT_TOPIC, value=event).add_errback(on_send_error)
                # flush на каждой итерации отдаёт сообщение сразу и сразу показывает ошибку
                producer.flush()
            except KafkaError as exc:
                log.error("брокер не отвечает (%s), остановка генератора", type(exc).__name__)
                raise SystemExit(1)
            update_stats(stats, event)
            log.info("отправлено тип %s temp %.1f hum %.1f время %s",
                     event["device_type_id"], event["temperature"],
                     event["humidity"], event["event_time"])
            # Сводка раз в STATS_EVERY событий, видно темп публикации и распределение типов
            if stats["total"] % STATS_EVERY == 0:
                log.info(summary_line(stats, time.monotonic() - start))
            time.sleep(PERIOD_SECONDS)
    except KeyboardInterrupt:
        # Штатная остановка по Ctrl+C или от main.py при завершении job
        log.info("остановка генератора, %s", summary_line(stats, time.monotonic() - start))
    finally:
        # Таймауты, чтобы при мёртвом брокере выход не висел дефолтные 60s
        try:
            producer.flush(timeout=5)
        except KafkaError:
            pass
        producer.close(timeout=5)


if __name__ == "__main__":
    run()

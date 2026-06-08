"""Точка входа проекта. Готовит справочник PostgreSQL, создаёт выходной топик,
по флагу поднимает генератор событий, создаёт окружение Flink, собирает пайплайн
и запускает его блокирующим env.execute (источник Kafka, lookup join, окно, sink
в Kafka и вывод агрегатов в консоль)."""
import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

import psycopg2

import config
from pipeline import job

ROOT = Path(__file__).resolve().parent
log = logging.getLogger("main")


def apply_sql():
    # Применяем ddl.sql и dml.sql справочника при старте, скрипты идемпотентны (решение 1.3)
    ddl = (ROOT / "ddl.sql").read_text(encoding="utf-8")
    dml = (ROOT / "dml.sql").read_text(encoding="utf-8")
    log.info("подключаюсь к PostgreSQL %s:%s, применяю ddl.sql и dml.sql", config.PG_HOST, config.PG_PORT)
    try:
        conn = psycopg2.connect(**config.pg_connect_kwargs())
    except psycopg2.OperationalError as exc:
        log.error("нет связи с PostgreSQL %s:%s, поднят ли docker compose up -d (%s)",
                  config.PG_HOST, config.PG_PORT, exc)
        raise SystemExit(1)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute(dml)
        # Читаем что записали, чтобы в логе был виден результат обращения к базе
        cur.execute("select id, type_name from device_types order by id")
        rows = cur.fetchall()
    conn.close()
    types = ", ".join(f"{r[0]} {r[1]}" for r in rows)
    log.info("справочник device_types наполнен, типов %s [%s]", len(rows), types)


def ensure_output_topic():
    # Создаём выходной топик заранее (решение 8.4). Одна партиция, чтобы порядок был
    # детерминирован при параллелизме 1. Идемпотентно, повторный запуск не падает
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import NoBrokersAvailable
    try:
        admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BROKER)
    except NoBrokersAvailable:
        log.error("нет связи с Kafka %s, поднят ли docker compose up -d", config.KAFKA_BROKER)
        raise SystemExit(1)
    try:
        if config.OUTPUT_TOPIC in admin.list_topics():
            log.info("топик %s уже есть", config.OUTPUT_TOPIC)
        else:
            admin.create_topics([NewTopic(config.OUTPUT_TOPIC, num_partitions=1, replication_factor=1)])
            log.info("топик %s создан", config.OUTPUT_TOPIC)
    finally:
        admin.close()


def start_producer():
    # Генератор отдельным процессом (решение 2.5), -u чтобы лог не буферился
    proc = subprocess.Popen(
        [sys.executable, "-u", str(ROOT / "iot_producer.py")],
        cwd=str(ROOT),
    )
    log.info("генератор запущен, pid %s", proc.pid)
    return proc


def stop_producer(proc):
    # Гасим генератор и дожидаемся выхода, иначе лог об остановке врёт
    if proc.poll() is not None:
        # Уже умер сам, например брокер был недоступен при старте
        log.warning("генератор завершился сам, код %s", proc.returncode)
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    log.info("генератор остановлен")


def build_flink_env():
    # Понятные подсказки до старта JVM, иначе ошибки PyFlink малочитаемы
    if not os.environ.get("JAVA_HOME"):
        log.warning("JAVA_HOME не задан, PyFlink может не найти JVM, нужен JDK 17")
    jars = config.jar_urls()
    if not jars:
        log.warning("в %s нет jar, запусти scripts/fetch_jars.ps1", config.JARS_DIR)

    # Импорт pyflink внутри функции, чтобы старт JVM был только при сборке окружения
    from pyflink.common import Configuration
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

    conf = Configuration()
    # Web UI на localhost:8081
    conf.set_integer("rest.port", 8081)
    conf.set_string("execution.runtime-mode", "STREAMING")
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    # Параллелизм 1. Топик в один раздел, при дефолтном параллелизме (по числу ядер)
    # простаивающие subtask источника не двигают watermark, и окно по event time
    # никогда не закрывается. Для локального демо так же детерминирован порядок
    env.set_parallelism(1)
    if jars:
        env.add_jars(*jars)
    tenv = StreamTableEnvironment.create(env)
    log.info("окружение Flink создано, параллелизм 1, подключено jar %s", len(jars))
    return env, tenv


def main():
    config.configure_logging()
    parser = argparse.ArgumentParser(description="IoT и Flink пайплайн")
    parser.add_argument("--with-producer", action="store_true",
                        help="поднять генератор событий в фоне")
    args = parser.parse_args()

    apply_sql()
    ensure_output_topic()

    producer = start_producer() if args.with_producer else None
    try:
        env, tenv = build_flink_env()
        started = job.build(env, tenv, config)
        if started:
            # Блокирующий запуск. Поднимает источник Kafka, lookup join, окно и sink
            # одним job, держит процесс до Ctrl+C
            env.execute("iot_aggregates")
        else:
            # Защитная ветка, штатно build собирает граф целиком и возвращает True
            log.info("граф не собран, потоковый запуск пропущен")
    except KeyboardInterrupt:
        # Штатная остановка по Ctrl+C, когда на шаге 9 появится блокирующий execute
        log.info("остановка по Ctrl+C")
    finally:
        if producer is not None:
            stop_producer(producer)


if __name__ == "__main__":
    main()

"""Сборка пайплайна. Источники, обогащение, окно и sink соединяются в один граф.
Sink-INSERT привязан к графу DataStream через StatementSet, чтобы всё поднялось
общим env.execute. Логируем каждый собранный узел, чтобы из терминала было видно,
как устроен пайплайн."""
import logging

from pipeline import sinks, sources, transforms

log = logging.getLogger("pipeline.job")


def _count_device_types(tenv, config):
    # Конечный select по JDBC, показывает что справочник в pg доступен Flink и его размер.
    # Считаем в Python, потому что count(*) в streaming дал бы changelog, а простой scan
    # по ограниченному источнику завершается сам
    n = 0
    with tenv.sql_query(f"select id from {config.DEVICE_TYPES_TABLE}").execute().collect() as it:
        for _ in it:
            n += 1
    return n


def build(env, tenv, config):
    """Собирает обработку на готовых env и tenv. Граф собран целиком (источники,
    обогащение, окно, kafka sink и вывод в консоль), возвращает True для env.execute()."""
    sources.register_events(env, tenv, config)
    log.info("источник событий собран, читаю топик %s из Kafka как DataStream (KafkaSource), "
             "перевожу в Table через from_data_stream, JSON и event_time разбираю в схеме, "
             "watermark с запасом %s сек", config.INPUT_TOPIC, config.WATERMARK_DELAY_SECONDS)

    sources.register_device_types(tenv, config)
    log.info("справочник %s подключён из PostgreSQL через JDBC, режим lookup, кеш FULL",
             config.DEVICE_TYPES_TABLE)
    rows = _count_device_types(tenv, config)
    log.info("через JDBC прочитал справочник из PostgreSQL, строк %s", rows)

    transforms.enrich_with_device_type(tenv, config)
    log.info("обогащение собрано, вид %s, lookup join событий со справочником по device_type_id "
             "(FOR SYSTEM_TIME AS OF), беру TypeName, события без типа отбрасываю", config.ENRICHED_VIEW)

    transforms.windowed_aggregates(tenv, config)
    log.info("окно собрано, вид %s, tumble %s мин по event time, считаю среднюю температуру, "
             "медиану влажности и число событий по типу", config.AGGREGATES_VIEW, config.WINDOW_MINUTES)

    # Схема агрегатов для наглядности. print_schema только планирует, обработку не запускает
    log.info("схема агрегатов на выходе окна")
    tenv.from_path(config.AGGREGATES_VIEW).print_schema()

    sinks.register_kafka_sink(tenv, config)
    log.info("приёмник Kafka собран через Table/SQL, пишу в топик %s, формат json (таблица %s)",
             config.OUTPUT_TOPIC, config.AGGREGATES_SINK_TABLE)

    # Источник это DataStream (from_data_stream), а sink через SQL INSERT. Чтобы оба
    # поднялись одним job по env.execute, собираем StatementSet и привязываем его к
    # графу DataStream через attach_as_datastream. Это и есть переход Table в DataStream
    # на стороне выхода
    stmt = tenv.create_statement_set()
    stmt.add_insert_sql(sinks.insert_aggregates_sql(config))
    stmt.attach_as_datastream()
    log.info("INSERT агрегатов в топик %s добавлен в граф через StatementSet", config.OUTPUT_TOPIC)

    # Склейка на стороне выхода. Тот же поток окна переводим Table в DataStream и печатаем
    # в консоль, чтобы результат было видно без kafka-консьюмера
    sinks.print_aggregates(tenv, config)
    log.info("дублирую агрегаты в консоль через переход Table в DataStream (to_data_stream)")

    log.info("граф пайплайна собран целиком, запускаю обработку, жду закрытия окон по event time")
    return True

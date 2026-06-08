"""Источники данных. Поток событий из Kafka и статичный справочник из PostgreSQL.
Импорт pyflink держим внутри функций, чтобы import пакета не поднимал JVM."""


def build_kafka_source(config):
    # Источник отдаёт сырую строку JSON. Разбор делаем в SQL, без Python (находка 5)
    from pyflink.common import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

    return KafkaSource.builder() \
        .set_bootstrap_servers(config.KAFKA_BROKER) \
        .set_topics(config.INPUT_TOPIC) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .build()


def register_events(env, tenv, config):
    # Сырой поток строк переводим в Table. JSON и event time разбираем в схеме через
    # column_by_expression, там же watermark. proc_time нужен для lookup join (4.3 B)
    from pyflink.common import WatermarkStrategy
    from pyflink.table import Schema, DataTypes

    source = build_kafka_source(config)
    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka iot_events")

    # 'T' в шаблоне экранируем двойным апострофом, это литерал внутри SQL-строки
    ts_pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS"
    schema = Schema.new_builder() \
        .column("f0", DataTypes.STRING()) \
        .column_by_expression("device_type_id", "cast(json_value(f0, '$.device_type_id') as int)") \
        .column_by_expression("temperature", "cast(json_value(f0, '$.temperature') as double)") \
        .column_by_expression("humidity", "cast(json_value(f0, '$.humidity') as double)") \
        .column_by_expression("event_time", f"to_timestamp(json_value(f0, '$.event_time'), '{ts_pattern}')") \
        .column_by_expression("proc_time", "proctime()") \
        .watermark("event_time", f"event_time - interval '{config.WATERMARK_DELAY_SECONDS}' second") \
        .build()

    table = tenv.from_data_stream(raw, schema)
    tenv.create_temporary_view(config.EVENTS_VIEW, table)
    return table


def register_device_types(tenv, config):
    # Справочник как JDBC-таблица Flink, читается из живого pg. Дальше работает
    # lookup-источником в temporal join (FOR SYSTEM_TIME AS OF) на шаге 6.
    # lookup.cache FULL грузит все строки в память один раз, справочник статичный
    # и крошечный, так нет обращения к pg на каждое событие
    tenv.execute_sql(f"""
        create table {config.DEVICE_TYPES_TABLE} (
            id int,
            type_name string,
            primary key (id) not enforced
        ) with (
            'connector' = 'jdbc',
            'url' = '{config.PG_JDBC_URL}',
            'table-name' = 'device_types',
            'username' = '{config.PG_USER}',
            'password' = '{config.PG_PASSWORD}',
            'lookup.cache' = 'FULL'
        )
    """)

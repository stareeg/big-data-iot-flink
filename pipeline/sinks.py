"""Приёмник. Запись агрегатов в выходной топик Kafka через Table/SQL.
Окно даёт append-поток (закрытые окна не меняются задним числом), поэтому обычный
коннектор kafka с format json, upsert не нужен."""


def register_kafka_sink(tenv, config):
    # Таблица-приёмник на коннекторе kafka, format json. Поля совпадают с view
    # агрегатов, append-поток окна ложится напрямую
    tenv.execute_sql(f"""
        create table {config.AGGREGATES_SINK_TABLE} (
            window_time string,
            device_type string,
            avg_temperature double,
            median_humidity double
        ) with (
            'connector' = 'kafka',
            'topic' = '{config.OUTPUT_TOPIC}',
            'properties.bootstrap.servers' = '{config.KAFKA_BROKER}',
            'format' = 'json'
        )
    """)


def insert_aggregates_sql(config):
    # INSERT агрегатов в приёмник. Запуск собирает job.build через StatementSet
    return (
        f"insert into {config.AGGREGATES_SINK_TABLE} "
        f"select window_time, device_type, avg_temperature, median_humidity "
        f"from {config.AGGREGATES_VIEW}"
    )


def print_aggregates(tenv, config):
    # Тот же поток окна переводим из Table в DataStream и печатаем в консоль, чтобы
    # результат было видно без kafka-консьюмера. Это явный переход Table в DataStream
    # на выходе (требование ТЗ про оба API). Поток append-only, печатает строки +I.
    # Строку собираем читаемой по-русски прямо в SQL, Python map на этой Windows сломан
    line = tenv.sql_query(f"""
        select concat(
            'окно ', window_time,
            ' тип ', device_type,
            ' событий ', cast(events_count as string),
            ' средняя темп ', cast(avg_temperature as string),
            ' медиана влажн ', cast(median_humidity as string)
        ) as line
        from {config.AGGREGATES_VIEW}
    """)
    ds = tenv.to_data_stream(line)
    ds.print()
    return ds

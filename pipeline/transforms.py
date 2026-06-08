"""Преобразования. Lookup join потока со справочником и оконная агрегация.
Разбор JSON и watermark живут в схеме источника (pipeline/sources.register_events),
потому что Python UDF на этой Windows не работает (см. logs/SUMMARY.md)."""


def enrich_with_device_type(tenv, config):
    # Lookup join событий со справочником по device_type_id через FOR SYSTEM_TIME AS OF
    # proc_time. Так атрибут event_time остаётся rowtime и доступен окну шага 7,
    # обычный join его потерял бы. INNER, событие без типа в справочнике отбрасываем,
    # генератор шлёт id 1..5 строго под dml, поэтому совпадают все
    enriched = tenv.sql_query(f"""
        select e.event_time, e.temperature, e.humidity, d.type_name as device_type
        from {config.EVENTS_VIEW} as e
        join {config.DEVICE_TYPES_TABLE} for system_time as of e.proc_time as d
            on e.device_type_id = d.id
    """)
    tenv.create_temporary_view(config.ENRICHED_VIEW, enriched)
    return enriched


def windowed_aggregates(tenv, config):
    # Tumble окно по event time, группировка по типу устройства. avg температуры
    # и точная медиана влажности встроенной PERCENTILE (Python UDF сломан).
    # Округляем до десятых, как точность датчика в генераторе, иначе avg тянет
    # длинный хвост знаков. window_time это метка начала минуты в местном времени.
    # events_count это число событий в окне, в выходной топик не идёт, нужен для
    # наглядности в консоли (сколько строк попало в окно). Шаг 7
    aggregates = tenv.sql_query(f"""
        select
            date_format(window_start, 'HH:mm') as window_time,
            device_type,
            round(avg(temperature), 1) as avg_temperature,
            round(percentile(humidity, 0.5), 1) as median_humidity,
            count(*) as events_count
        from table(
            tumble(table {config.ENRICHED_VIEW}, descriptor(event_time),
                   interval '{config.WINDOW_MINUTES}' minute)
        )
        group by window_start, window_end, device_type
    """)
    tenv.create_temporary_view(config.AGGREGATES_VIEW, aggregates)
    return aggregates

-- Справочник типов IoT-устройств
-- Применяется из main.py при старте, повторный запуск безопасен
create table if not exists device_types (
    id integer primary key,
    type_name text not null
);

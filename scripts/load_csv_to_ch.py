import pandas as pd
from clickhouse_connect import get_client

# Параметры подключения к ClickHouse
CLICKHOUSE_HOST = 'clickhouse'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'myuser'
CLICKHOUSE_PASSWORD = 'mypassword'
CLICKHOUSE_DATABASE = 'default'

# Путь к CSV
CSV_PATH = '/opt/airflow/data/sales.csv'
TABLE_NAME = 'sales'

# Схема таблицы (по структуре CSV)
CREATE_TABLE_SQL = f'''
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    year UInt16,
    anzsic String,
    anzsic_descriptor String,
    variable1 String,
    variable2 String,
    category String,
    units String,
    magnitude String,
    source String,
    data_value Float64
) ENGINE = MergeTree()
ORDER BY (year, anzsic, variable1, variable2, category)
'''

def main():
    # Чтение данных
    df = pd.read_csv(CSV_PATH)

    # Подключение к ClickHouse
    client = get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )

    # Создание таблицы
    client.command(CREATE_TABLE_SQL)

    # Проверка наличия данных
    row_count = client.query(f"SELECT count() FROM {TABLE_NAME}").result_rows[0][0]
    if row_count > 0:
        print(f"Таблица {TABLE_NAME} уже содержит данные ({row_count} строк). Загрузка пропущена.")
        return

    # Загрузка данных
    client.insert_df(TABLE_NAME, df)
    print(f"Загружено {len(df)} строк в таблицу {TABLE_NAME}")

if __name__ == '__main__':
    main()

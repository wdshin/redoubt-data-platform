from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from time import time
import logging
import requests


@dag(
    schedule_interval="*/5 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=['ton', 'jettons', 'datamart', 'mvp', 'cex', 'mexc']
)
def mexc_fetcher():
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="ton_db",
        sql=[
            """
        CREATE TABLE IF NOT EXISTS mexc_stat (
            id bigserial NOT NULL primary key,
            check_time timestamp with time zone NOT NULL, 
            address varchar,
            symbol varchar, 
            price decimal(40, 20),
            market_volume_ton_24 decimal(40, 0)                              
        );""",
            """
        CREATE INDEX IF NOT EXISTS mexc_stat_idx
        ON mexc_stat (address, check_time DESC);            
            """
        ]
    )


    def fetch_info_mexc():
        postgres_hook = PostgresHook(postgres_conn_id="ton_db")
        to_insert = []
        now = int(time())
        fnz = requests.get("https://api.mexc.com/api/v3/ticker/24hr?symbol=FNZUSDT").json()
        usdt = requests.get("https://api.mexc.com/api/v3/ticker/24hr?symbol=TONUSDT").json()
        fnz_ton_volume = round(float(fnz['quoteVolume']) / float(usdt['lastPrice']))
        fnz_ton_price = float(fnz['lastPrice']) / float(usdt['lastPrice'])
        insert_sql = f"""INSERT INTO mexc_stat(address, check_time, symbol,
        price, market_volume_ton_24 )
         VALUES ('EQDCJL0iQHofcBBvFBHdVG233Ri2V4kCNFgfRT-gqAd3Oc86', now(), 'FNZ', 
         {fnz_ton_price}, {fnz_ton_volume});
        """
        postgres_hook.run(insert_sql, autocommit=True)

    fetch_info_mexc_task = PythonOperator(
        task_id=f'fetch_info_mexc',
        python_callable=fetch_info_mexc
    )

    create_tables >> fetch_info_mexc_task


mexc_fetcher_dag = mexc_fetcher()
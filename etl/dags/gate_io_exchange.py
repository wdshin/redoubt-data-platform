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
    tags=['ton', 'jettons', 'datamart', 'mvp', 'cex', 'gate.io']
)
def gateio_fetcher():
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="ton_db",
        sql=[
            """
        CREATE TABLE IF NOT EXISTS gateio_stat (
            id bigserial NOT NULL primary key,
            check_time timestamp with time zone NOT NULL, 
            address varchar,
            symbol varchar, 
            price decimal(40, 20),
            market_volume_ton_24 decimal(40, 0)                              
        );""",
            """
        CREATE INDEX IF NOT EXISTS gateio_stat_idx
        ON gateio_stat (address, check_time DESC);            
            """
        ]
    )


    def fetch_info_gateio():
        postgres_hook = PostgresHook(postgres_conn_id="ton_db")
        # Get USDT / TON price from existing datamart using oUSDT price
        usdt_price = float(postgres_hook.get_first("""
        select price from top_jettons_datamart tjd 
        where address = 'EQC_1YoM8RBixN95lz7odcF3Vrkc_N8Ne7gQi7Abtlet_Efi'
        order by build_time desc limit 1
        """)[0])
        fnz = requests.get("https://api.gateio.ws/api/v4/spot/tickers?currency_pair=FNZ_USDT").json()[0]
        fnz_ton_volume = round(float(fnz['quote_volume']) * usdt_price)
        fnz_ton_price = float(fnz['last']) * usdt_price
        insert_sql = f"""INSERT INTO gateio_stat(address, check_time, symbol,
        price, market_volume_ton_24 )
         VALUES ('EQDCJL0iQHofcBBvFBHdVG233Ri2V4kCNFgfRT-gqAd3Oc86', now(), 'FNZ', 
         {fnz_ton_price}, {fnz_ton_volume});
        """
        postgres_hook.run(insert_sql, autocommit=True)

    fetch_info_gateio_task = PythonOperator(
        task_id=f'fetch_info_gateio',
        python_callable=fetch_info_gateio
    )

    create_tables >> fetch_info_gateio_task


gateio_fetcher_dag = gateio_fetcher()
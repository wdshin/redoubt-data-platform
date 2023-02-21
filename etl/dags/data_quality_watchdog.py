import dataclasses
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import logging
import requests

@dataclasses.dataclass
class Check:
    name: str
    sql: str
    limit: int
    condition: str = "<"  # < or >

CHECKS = [
    Check(
        name='Gate.io lag',
        sql="select round(extract(epoch from now() - max(check_time))) as lag from gateio_stat",
        limit=400
    ),
    Check(
        name='MEXC lag',
        sql="select round(extract(epoch from now() - max(check_time))) as lag from mexc_stat",
        limit=400
    ),
    Check(
        name='TON Rocket lag',
        sql="select round(extract(epoch from now() - max(check_time))) as lag from ton_rocket_stat",
        limit=400
    ),
    Check(
        name='Top jetton datamart lag',
        sql="select round(extract(epoch from now() - max(build_time))) as lag from top_jettons_datamart",
        limit=1800
    ),
    Check(
        name='Last DEX swap time',
        sql="select round(extract(epoch from now() - max(swap_time))) as lag from mview_dex_swaps",
        limit=3600
    ),
    Check(
        name='Last block from blockchain',
        sql="select round(extract(epoch from now() - to_timestamp(max(utime)))) as lag from transactions",
        limit=60
    ),
    Check(
        name='Jetton operations',
        sql="""
        with jetton_actions as (
            select max(utime) as utime from jetton_transfers
            union all
            select max(utime) as utime from jetton_burn
            union all
            select max(utime) as utime from jetton_mint
        )
        select round(extract(epoch from now() - to_timestamp(max(utime)))) as lag from jetton_actions
        """,
        limit=1800
    ),
    Check(
        name='Parse queue size',
        sql="select count(1) from parse_outbox",
        limit=3000
    ),
    Check(
        name='Unparsed accounts size',
        sql="select count(1) from accounts where last_check_time is null",
        limit=200
    )
]

@dag(
    schedule_interval="*/30 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=['datamart', 'data-quality', 'watchdog']
)
def data_quality_watchdog():

    def send(msg):
        telegram_hook = TelegramHook(telegram_conn_id="telegram_watchdog_conn")
        telegram_hook.send_message({"text": msg})

    def check_jetton_api():
        try:
            jettons = requests.get("https://api.redoubt.online/v1/jettons/top").json()
        except Exception as e:
            logging.error(f"Jettons API is down: {e}")
            send(f"📛 Jettons API is down: {e}")

        for jetton in jettons['jettons']:
            address = jetton['address']
            status_code = requests.get(f"https://api.redoubt.online/v1/jettons/image/{address}").status_code
            if status_code != 200:
                logging.error(f"Unable to fetch image for {jetton['name']}: {status_code}")
                send(f"📛Unable to fetch image for {jetton['name']} ({address}): {status_code}")


    check_jetton_api_task = PythonOperator(
        task_id=f'check_jetton_api',
        python_callable=check_jetton_api
    )

    def check_datamart_lag():
        postgres_hook = PostgresHook(postgres_conn_id="ton_db")
        for check in CHECKS:
            value = postgres_hook.get_first(check.sql)[0]
            if (check.condition == "<" and value > check.limit) or (check.condition == ">" and value < check.limit):
                send(f"🕗 {check.name}: {value} (limit: {check.limit})")


    check_datamart_lag_task = PythonOperator(
        task_id=f'check_datamart_lag',
        python_callable=check_datamart_lag
    )


    check_jetton_api_task >> check_datamart_lag_task


data_quality_watchdog_dag = data_quality_watchdog()
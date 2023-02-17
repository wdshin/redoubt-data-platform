from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook

from datetime import datetime
from time import time
import logging
import requests


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

    [check_jetton_api]


data_quality_watchdog_dag = data_quality_watchdog()
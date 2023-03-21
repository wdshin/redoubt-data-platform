from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.telegram.hooks.telegram import TelegramHook

from datetime import datetime
from time import time
import logging
import requests


# Mapping between pairs and addresses
PAIRS = {
    "SCALE-TONCOIN": {
        "address": "EQBlqsm144Dq6SjbPI4jjZvA1hqTIP3CvHovbIfW_t-SCALE",
        "token_position": 0,
        "token": "SCALE",
        "decimals": 9
    },
    "BOLT-TONCOIN": {
        "address": "EQD0vdSA_NedR9uvbgN9EikRX-suesDxGeFg69XQMavfLqIw",
        "token_position": 0,
        "token": "BOLT",
        "decimals": 9
    },
    "TGR-TONCOIN": {
        "address": "EQAvDfWFG0oYX19jwNDNBBL1rKNT9XfaGP9HyTb5nb2Eml6y",
        "token_position": 0,
        "token": "TGR",
        "decimals": 9
    },
    "TAKE-TONCOIN": {
        "address": "EQBzyesZ3p1WGNrggNSJi6JFK3vr0GhqJp4gxker9oujjcuv",
        "token_position": 0,
        "token": "TAKE",
        "decimals": 9
    },
    "HEDGE-TONCOIN": {
        "address": "EQBiJ8dSbp3_YAb_KuC64zCrFqQTsFbUee5tbzr5el_HEDGE",
        "token_position": 0,
        "token": "HEDGE",
        "decimals": 9
    },
    "KOTE-TONCOIN": {
        "address": "EQBlU_tKISgpepeMFT9t3xTDeiVmo25dW_4vUOl6jId_BNIj",
        "token_position": 0,
        "token": "KOTE",
        "decimals": 0, # actually nKOTE
        "price_multiplier": 1000000000  # convert from nKOTE/TON to KOTE/TON
    },
    "TNX-TONCOIN": {
        "address": "EQB-ajMyi5-WKIgOHnbOGApfckUGbl6tDk3Qt8PKmb-xLAvp",
        "token_position": 0,
        "token": "TNX",
        "decimals": 9
    },
    "GRBS-TONCOIN": {
        "address": "EQBj7uoIVsngmS-ayOz1nHENjZkjTt5mXB4uGa83hmcqq2wA",
        "token_position": 0,
        "token": "GRBS",
        "decimals": 9
    },
    "TONCOIN-oUSDT": {
        "address": "EQC_1YoM8RBixN95lz7odcF3Vrkc_N8Ne7gQi7Abtlet_Efi",
        "token_position": 1,
        "token": "oUSDT",
        "decimals": 6
    },
    "AMBR-TONCOIN": {
        "address": "EQCcLAW537KnRg_aSPrnQJoyYjOZkzqYp6FVmRUvN1crSazV",
        "token_position": 0,
        "token": "AMBR",
        "decimals": 9
    },
    "KISS-TONCOIN": {
        "address": "EQAE8sAbvxMoIrN2tAuOe4vI5H4JhnHI-zn2VoRy-agH7BCn",
        "token_position": 0,
        "token": "KISS",
        "decimals": 9
    },
    "JBCT-TONCOIN": {
        "address": "EQAvvEdq4x5K6owjsMI4X6AxKkTBXVKIVkiSHDLt6i2LFnnQ",
        "token_position": 0,
        "token": "JBCT",
        "decimals": 9
    },
    "IVS-TONCOIN": {
        "address": "EQAdSQFNyPCbT14QKPBZzpQkf4MC5AvYsiNYZolymzrckMBs",
        "token_position": 0,
        "token": "IVS",
        "decimals": 9
    },
    "LAVE-TONCOIN": {
        "address": "EQBl3gg6AAdjgjO2ZoNU5Q5EzUIl8XMNZrix8Z5dJmkHUfxI",
        "token_position": 0,
        "token": "LAVE",
        "decimals": 9
    },
    "VIRUS-TONCOIN": {
        "address": "EQAA7ckPtTtZ5msFtmIem3-AAuUG7HVmn9_ehgD_qB37Hty4",
        "token_position": 0,
        "token": "VIRUS",
        "decimals": 9
    },
    "DHD-TONCOIN": {
        "address": "EQBCFwW8uFUh-amdRmNY9NyeDEaeDYXd9ggJGsicpqVcHq7B",
        "token_position": 0,
        "token": "DHD",
        "decimals": 9
    },
    "KINGY-TONCOIN": {
        "address": "EQC-tdRjjoYMz3MXKW4pj95bNZgvRyWwZ23Jix3ph7guvHxJ",
        "token_position": 0,
        "token": "KINGY",
        "decimals": 9
    }
}

@dag(
    schedule_interval="*/5 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=['ton', 'jettons', 'datamart', 'mvp', 'cex', 'ton-rocket']
)
def ton_rocket_fetcher():
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="ton_db",
        sql=[
            """
        CREATE TABLE IF NOT EXISTS ton_rocket_stat (
            id bigserial NOT NULL primary key,
            check_time timestamp with time zone NOT NULL, 
            address varchar,
            symbol varchar, 
            price decimal(40, 20),
            market_volume_ton_24 decimal(40, 0)                              
        );""",
            """
        CREATE INDEX IF NOT EXISTS ton_rocket_stat_idx
        ON ton_rocket_stat (address, check_time DESC);            
            """
        ]
    )


    def fetch_info_rocket_exchange():
        postgres_hook = PostgresHook(postgres_conn_id="ton_db")
        to_insert = []
        now = int(time())
        all_pairs = requests.get("https://trade.ton-rocket.com/pairs", headers={"Content-type": "application/json"}).json()
        assert all_pairs['success'], all_pairs
        for pair in all_pairs['data']:
            pair_info = PAIRS.get(pair['name'], None)
            if pair_info is None:
                logging.warning(f"pair not found {pair}")
                telegram_hook = TelegramHook(telegram_conn_id="telegram_watchdog_conn")
                telegram_hook.send_message({"text": f"New pair on TON Rocket: {pair}"})
                continue
            ton_key = 'quoteVolume24h' if pair_info['token_position'] == 0 else 'mainVolume24h'
            volume_ton = round(pair[ton_key])
            price_avg = (pair['buyPrice'] + pair['sellPrice']) / 2
            if 'price_multiplier' in pair_info:
                price_avg = price_avg * pair_info['price_multiplier']
            if pair_info['token_position'] == 1:
                price_avg = 1. / price_avg
            
            to_insert.append(f"""('{pair_info['address']}', to_timestamp({now}), '{pair_info['token']}', 
                    {price_avg}, {volume_ton})""")

        insert_sql = """INSERT INTO ton_rocket_stat(address, check_time, symbol,
        price, 
        market_volume_ton_24 ) VALUES  %s ;""" % ",".join(to_insert)
        postgres_hook.run(insert_sql, autocommit=True)

    fetch_info_rocket_exchange_task = PythonOperator(
        task_id=f'fetch_info_rocket_exchange',
        python_callable=fetch_info_rocket_exchange
    )

    create_tables >> fetch_info_rocket_exchange_task


ton_rocket_fetcher_dag = ton_rocket_fetcher()
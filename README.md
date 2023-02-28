# re:doubt data-platform

## Dependencies

Data platform depends on [ton-indexer](https://github.com/re-doubt/ton-indexer).

## ETL

ETL based on Airflow. Jobs list:

* [rebuild-top-jettons-datamart](etl/dags/rebuild-top-jettons-datamart.py) - creates datamart with top jettons by market volume
* [TON Rocket indexer](etl/dags/ton_rocket_exchange.py) - index last 24h stats from TON Rocket swaps
* [MEXC indexer](etl/dags/mexc_exchange.py) - index last 24h stats from MEXC (FNZ trades)
* [Gate.io indexer](etl/dags/gate_io_exchange.py) - index last 24h stats from Gate.io (FNZ trades)
* [Data quality watchdog](etl/dags/data_quality_watchdog.py) - data quality watchdog
* [TVL datamart updater](etl/dags/tvl-datamart.py) - updates TVL values for DEXs pools

Variables and connectins:
* ``contracts_executor_url`` - url for [contracts-executor](https://github.com/re-doubt/ton-indexer/tree/master/contracts-executor)
(default value is http://contracts-executor:9090/execute)
* ``ton_db`` - main DB (from ton-indexer)
* ``telegram_watchdog_conn`` - Telegram credentials for notifications
* ``alerts_tvl_delta_min_pool`` - min LP size for delta alerts
* ``alerts_tvl_delta_percent`` - LP delta (percents) for alerts 

## Backend

To deploy:

```shell
cd backend
docker compose up -d
```
                     
Methods:
* ``GET /v1/jettons/top`` returns datamart with top Jettons and volume by platform
* ``GET /v1/jettons/image/{address}`` fetches jetton image and returns as image content

# re:doubt data-platform

## ETL

ETL based on Airflow. Jobs list:

* [rebuild-top-jettons-datamart](etl/dags/rebuild-top-jettons-datamart.py) - creates datamart with top jettons by market volume

## Backend

To deploy:

```shell
cd backend
docker compose up -d
```
                     
Methods:
* ``GET /v1/jettons/top`` returns datamart with top Jettons
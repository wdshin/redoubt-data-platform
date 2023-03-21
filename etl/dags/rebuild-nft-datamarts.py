from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

@dag(
    schedule_interval="*/20 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=['ton', 'nft', 'datamart']
)
def rebuild_nft_datamarts():
    actual_owners_mview = PostgresOperator(
        task_id="actual_owners_mview",
        postgres_conn_id="ton_db",
        sql=[
            """
        create materialized view if not exists public.mview_nft_actual
        as
        with last_known_owner as (
          select ni.address, ni.owner, account_state.last_tx_lt, ni.collection  from nft_item ni join account_state using(state_id)
        ), last_transfer as (
          select  nt.nft_item as address, nt.new_owner as owner, created_lt from nft_transfers nt 
          where created_lt = (select max(nt2.created_lt) from nft_transfers nt2 where nt2.nft_item = nt.nft_item and nt2.successful) 
        ), actual_owners as (
         select lo.address, collection, coalesce(lt.owner, lo.owner) as owner
         from last_known_owner lo
         left join last_transfer lt on lo.address = lt.address and lt.created_lt > lo.last_tx_lt
        )
        select ao.address, collection, coalesce(nis.owner, ao.owner) as owner, 
        nis.price as price
        from actual_owners ao
        left join public.nft_item_sale nis on ao.owner = nis.address                           
        """,
            """
        create unique index if not exists mview_nft_actual_addr_idx on mview_nft_actual(address);            
            """,
            """
        refresh materialized view concurrently mview_nft_actual;        
            """
        ]
    )


    actual_owners_mview



rebuild_nft_datamarts_dag = rebuild_nft_datamarts()
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

@dag(
    schedule_interval="*/10 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=['ton', 'jettons', 'datamart', 'mvp']
)
def rebuild_top_jettons_datamart():
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="ton_db",
        sql=[
            """
        CREATE TABLE IF NOT EXISTS top_jettons_datamart (
            id bigserial NOT NULL primary key,
            build_time timestamp with time zone NOT NULL, 
            address varchar UNIQUE,
            creation_time timestamp with time zone NOT NULL,
            symbol varchar, 
            name varchar,
            description varchar,
            image_url varchar,
            price decimal(40, 20),
            market_volume_ton decimal(40, 0),
            market_volume_rank bigint,
            active_owners_24 bigint,
            total_holders bigint                              
        );""",
            """
        CREATE INDEX IF NOT EXISTS top_jettons_datamart_build_idx
        ON top_jettons_datamart (build_time DESC, market_volume_rank ASC);            
            """
        ]
    )

    ## TODO add mat view code here
    refresh_dex_swaps = PostgresOperator(
        task_id="refresh_dex_swaps",
        postgres_conn_id="ton_db",
        sql=[
            """
            refresh materialized view mview_dex_swaps;                    
            """
        ]
    )

    add_current_top_jettons = PostgresOperator(
        task_id="add_current_top_jettons",
        postgres_conn_id="ton_db",
        sql=[
            """
        insert into top_jettons_datamart(build_time, address, 
          creation_time, symbol, name, description, image_url, market_volume_ton,
          market_volume_rank, active_owners_24, total_holders           
        )
        with enriched as ( -- add jetton symbol
          select swaps.*, jm_src.symbol as src, jm_dst.symbol as dst from mview_dex_swaps swaps
          join jetton_master jm_src on jm_src.address  = swaps.swap_src_token
          join jetton_master jm_dst on jm_dst.address  = swaps.swap_dst_token
          where swap_time  > now() - interval '1 day'
        ), trades_in_ton as ( -- only DeDust swaps for now
        select
          originated_msg_id, swap_time, swap_src_owner as swap_owner, swap_src_token as token,
          swap_src_amount as amount_token, swap_dst_amount as amount_ton,
          'sell' as direction
          from enriched where dst = 'JTON'
        union all
        select
          originated_msg_id, swap_time, swap_src_owner as swap_owner, swap_dst_token as token,
          swap_dst_amount as amount_token, swap_src_amount as amount_ton,
          'buy' as direction
          from enriched where src = 'JTON'
        ), market_volume as  (
          select token, round(sum(amount_ton) / 1000000000) as market_volume_ton from trades_in_ton
          group by 1
        ), market_volume_rank as (
          select *, rank() over(order by market_volume_ton desc) as market_volume_rank from market_volume
        ), last_trades_ranks as (
        select
          swap_time, swap_src_token as token, swap_src_amount as amount_token, swap_dst_amount as amount_ton,
          'sell' as direction, rank() over(partition by swap_src_token order by swap_time desc) as rank
          from enriched where dst = 'JTON'
        union all
        select
          swap_time, swap_dst_token as token, swap_dst_amount as amount_token, swap_src_amount as amount_ton,
          'buy' as direction, rank() over(partition by swap_dst_token order by swap_time desc) as rank
          from enriched where src = 'JTON'
        ), prices as (
          select token, sum(amount_ton) / sum(amount_token) as price_raw  from last_trades_ranks
          where rank < 4 -- last 3 trades
          group by 1
        ), min_data as (
          select jm.address as token, to_timestamp(min(t.utime)) as creation_time
          from jetton_master jm   
          join messages m on m.destination  = jm.address  
          join transactions t on t.tx_id = m.in_tx_id 
          group by 1
        ), datamart as (
          select mv.*, md.creation_time, jm.symbol, jm.name, jm.description, case
            when coalesce(jm.decimals, 9) = 9 then price_raw
            when jm.decimals < 9 then price_raw / (pow(10, 9 - jm.decimals))
            else price_raw * (pow(10, 9 - jm.decimals))
          end as price, jm.decimals  from market_volume_rank as mv
          join jetton_master jm on jm.address  = mv.token 
          join prices on prices.token = mv.token
          join min_data md on md.token = mv.token
          where market_volume_rank > 100 or market_volume_ton > 10
        )
        select  now() as build_time, token as address, 
          creation_time, symbol, name, description, 
          '' as image_url, -- TODO 
          market_volume_ton, market_volume_rank, 
          0 as active_owners_24, 0 as total_holders -- TODO   
        from datamart                                
            """
        ]
    )

    create_tables >>  refresh_dex_swaps >> add_current_top_jettons



rebuild_top_jettons_datamart_dag = rebuild_top_jettons_datamart()
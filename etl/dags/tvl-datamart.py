from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

@dag(
    schedule_interval="@hourly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=['ton', 'jettons', 'datamart', 'tvl', 'dex']
)
def tvl_datamart():
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="ton_db",
        sql=[
            """
            create table if not exists lp_info (
              id bigserial primary key,
              platform varchar,
              address varchar,
              type varchar
            );""",
            """
        create unique index if not exists lp_info_idx1 on lp_info(address);            
            """,
            """
        CREATE TABLE IF NOT EXISTS tvl_history_datamart (
            id bigserial NOT NULL primary key,
            build_time timestamp with time zone NOT NULL,
            platform varchar, 
            address varchar,
            jetton_a varchar,
            jetton_b varchar,
            tvl_ton decimal(20, 0)                              
        );""",
        ]
    )


    refresh_mview_pools_balances = PostgresOperator(
        task_id="refresh_mview_pools_balances",
        postgres_conn_id="ton_db",
        sql=[
            """
            create materialized view if not exists mview_dex_pools_balances
            as
            with pools as (
              select distinct address from lp_info
            ), pool2jetton_counts as (
              select destination_owner as address, jw.jetton_master, count(1) as transfers_cnt from jetton_transfers jt
              join pools on pools.address = jt.destination_owner 
              join jetton_wallets jw on jw.address  = jt.source_wallet 
              where to_timestamp(jt.utime) > now() - interval '30 days'
              group by 1, 2
            ), pool2jetton_ranks as (
              select address, jetton_master, transfers_cnt, 
              rank() over(partition by address order by transfers_cnt desc) as activity_rank
              from pool2jetton_counts
            ),token1 as (
              select address, jetton_master from pool2jetton_ranks where activity_rank = 1
            ),token2 as (
              select address, jetton_master from pool2jetton_ranks where activity_rank = 2
            ), pools_ordered as (
                select platform, type, address, case
                    when type = 'ton2token' then token1.jetton_master
                    when type = 'token2token' and token2.jetton_master is not null 
                      and  token2.jetton_master >  token1.jetton_master
                      then token2.jetton_master
                    when type = 'token2token' and token2.jetton_master is not null 
                      and  token2.jetton_master <  token1.jetton_master
                      then token1.jetton_master
                end as jetton_a,
                case
                    when type = 'ton2token' then 'EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c' -- TON
                    when type = 'token2token' and token2.jetton_master is not null 
                      and  token2.jetton_master >  token1.jetton_master
                      then token1.jetton_master
                    when type = 'token2token' and token2.jetton_master is not null 
                      and  token2.jetton_master <  token1.jetton_master
                      then token2.jetton_master
                end as jetton_b
                from lp_info 
                join token1 using(address)
                left join token2 using(address)
            ), pools_balances_ranks as (
              select *, rank() over(partition by address order by last_tx_lt desc) as state_rank 
              from account_state state
              join pools_ordered using(address)
              where jetton_b = 'EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c'
            ), last_balances_ton as (
              select address, last_tx_lt, balance from pools_balances_ranks where state_rank = 1
            ), incoming_ton as (
              select address, sum(m.value - t.fee) as incoming from last_balances_ton l
              join messages m on m.destination = l.address
              join transactions t on t.tx_id = m.in_tx_id 
              where t.action_result_code = 0 and t.compute_exit_code = 0 and t.lt > l.last_tx_lt
              group by 1
            ), outgoing_ton as (
              select address, sum(m.value + t.fee) as outgoing from last_balances_ton l
              join messages m on m.source = l.address
              join transactions t on t.tx_id = m.out_tx_id  
              where t.action_result_code = 0 and t.compute_exit_code = 0 and t.lt > l.last_tx_lt
              group by 1
            ), current_balances_ton as (
              select address, balance + coalesce(incoming, 0) - coalesce(outgoing, 0) as balance_ton from last_balances_ton
              left join incoming_ton using(address)
              left join outgoing_ton using(address)
            ), pools_with_balances as (
              select pools.*, mjb_a.balance as balance_a,
              case 
                  when type = 'ton2token' then cbt.balance_ton 
                  else mjb_b.balance 
              end as balance_b 
              from pools_ordered pools
              left join jetton_wallets jw_a on jw_a."owner"  = pools.address and jw_a.jetton_master = pools.jetton_a
              left join mview_jetton_balances mjb_a on mjb_a.wallet_address = jw_a.address
              left join jetton_wallets jw_b on jw_b."owner"  = pools.address and jw_b.jetton_master = pools.jetton_b
              left join mview_jetton_balances mjb_b on mjb_b.wallet_address = jw_b.address
              left join current_balances_ton cbt on cbt.address = pools.address
              where jetton_a is not null and jetton_b is not null
            )
            select distinct * from pools_with_balances where balance_a > 0 and balance_b > 0
            """,
            """
            create unique index if not exists mview_dex_pools_balances_address on mview_dex_pools_balances(address);            
            """,
            """
            refresh materialized view concurrently mview_dex_pools_balances;                    
            """
        ]
    )

    create_history_entry = PostgresOperator(
        task_id="create_history_entry",
        postgres_conn_id="ton_db",
        sql=[
            """
            create or replace view view_dex_tvl_current
            as
            with prices_v1 as (
              select jetton_a as address, (sum(balance_b) / sum(balance_a)) as price
              from mview_dex_pools_balances
              where jetton_b = 'EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c' 
                    or jetton_b = 'EQBPAVa6fjMigxsnHF33UQ3auufVrg2Z8lBZTY9R-isfjIFr' -- JTON
                    or jetton_b = 'EQDQoc5M3Bh8eWFephi9bClhevelbZZvWhkqdo80XuY_0qXv' -- WTON
                    or jetton_b = 'EQCM3B12QK1e4yZSf8GtBRT0aLMNyEsBc_DhVfRRtOEffLez' -- pTON
                    or jetton_b = 'EQCajaUU1XXSAjTD-xOV7pE49fGtg4q8kF3ELCOJtGvQFQ2C' -- WTON from megaton
              group by 1
            ), prices_v2 as (
              select jetton_a as address, avg(balance_b / balance_a * prices_v1.price) as price
              from mview_dex_pools_balances b join prices_v1 on b.jetton_b = prices_v1.address
              group by 1
            ), prices_all as (
              select address, coalesce(prices_v1.price, prices_v2.price) as price from prices_v1
              full outer join prices_v2 using(address)
            ),
            datamart as (
              select platform, type, b.address, jetton_a, jetton_b ,
              case 
                when jetton_b = 'EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c' 
                    or jetton_b = 'EQBPAVa6fjMigxsnHF33UQ3auufVrg2Z8lBZTY9R-isfjIFr' -- JTON
                    or jetton_b = 'EQDQoc5M3Bh8eWFephi9bClhevelbZZvWhkqdo80XuY_0qXv' -- WTON
                    or jetton_b = 'EQCM3B12QK1e4yZSf8GtBRT0aLMNyEsBc_DhVfRRtOEffLez' -- pTON
                    or jetton_b = 'EQCajaUU1XXSAjTD-xOV7pE49fGtg4q8kF3ELCOJtGvQFQ2C' -- WTON from megaton
                  then balance_b * 2
                when jetton_a = 'EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c' 
                    or jetton_a = 'EQBPAVa6fjMigxsnHF33UQ3auufVrg2Z8lBZTY9R-isfjIFr' -- JTON
                    or jetton_a = 'EQDQoc5M3Bh8eWFephi9bClhevelbZZvWhkqdo80XuY_0qXv' -- WTON
                    or jetton_a = 'EQCM3B12QK1e4yZSf8GtBRT0aLMNyEsBc_DhVfRRtOEffLez' -- pTON
                    or jetton_a = 'EQCajaUU1XXSAjTD-xOV7pE49fGtg4q8kF3ELCOJtGvQFQ2C' -- WTON from megaton
                  then balance_a * 2
                when p1.price is not null and p2.price is not null
                  then balance_a * p1.price + balance_b * p2.price
                when p1.price is not null and p2.price is null
                  then balance_a * p1.price * 2
                when p2.price is not null and p1.price is null
                  then balance_a * p2.price * 2
                else null
              end as tvl_ton, 
              p1.price, p2.price,
              balance_a * p1.price, balance_b * p2.price
              from mview_dex_pools_balances b
              left join prices_all p1 on p1.address = b.jetton_a
              left join prices_all p2 on p2.address = b.jetton_b
            ), symbols as (
             select address, symbol from jetton_master
             union all
             select 'EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c' as address, 'TON' as symbol
            )
            select datamart.address, platform, jetton_a, jetton_b,
            round(tvl_ton / 1000000000) as tvl_ton
            from datamart
            where tvl_ton is not null            
            """,
            """
            insert into tvl_history_datamart(build_time, platform, address, jetton_a, jetton_b, tvl_ton)
            select now(), platform, address, jetton_a, jetton_b, tvl_ton from view_dex_tvl_current            
            """
            ]
    )

    create_tables >>  refresh_mview_pools_balances >> create_history_entry



tvl_datamart_dag = tvl_datamart()
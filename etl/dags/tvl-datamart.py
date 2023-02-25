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
            """
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
            select * from pools_with_balances where balance_a > 0 and balance_b > 0
            """,
            """
            create unique index if not exists mview_dex_pools_balances_address on mview_dex_pools_balances(address);            
            """,
            """
            refresh materialized view concurrently mview_dex_pools_balances;                    
            """
        ]
    )

    create_tables >>  refresh_mview_pools_balances



tvl_datamart_dag = tvl_datamart()
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

@dag(
    schedule_interval="*/20 * * * *",
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
            address varchar,
            creation_time timestamp with time zone NOT NULL,
            symbol varchar, 
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


    refresh_dex_swaps = PostgresOperator(
        task_id="refresh_dex_swaps",
        postgres_conn_id="ton_db",
        sql=[
            """
            create table if not exists dex_pools_info (
              id bigserial primary key,
              platform varchar,
              address varchar,
              type varchar,
              sub_op bigint
            );
            """,
            """
            create index if not exists dex_pools_info_idx1 on dex_pools_info(type, address);
            """,
            """
            create unique index if not exists dex_pools_info_idx2 on dex_pools_info(platform, type, address);
            """,
            """
            create materialized view if not exists mview_dex_swaps
            as
            with transfers as (
              select m.created_lt, jw.jetton_master, to_timestamp(tx.utime) as swap_time, jt.*  from jetton_transfers jt 
              join jetton_wallets jw on jw.address =jt.source_wallet 
              join messages m on m.msg_id  = jt.msg_id  
              join transactions tx on tx.tx_id  = m.in_tx_id 
              where -- to_timestamp(tx.utime) > now() - interval '1 day' and 
              jt.successful  = true
            ),
            swaps as (
             select pool.platform, jt1.originated_msg_id,
               jt1.swap_time,
               jt1.source_owner as swap_src_owner, jt1.jetton_master as swap_src_token, 
               jt1.amount as swap_src_amount, jt1.query_id as swap_src_query_id, jt1.created_lt as swap_src_lt,
               
               jt2.destination_owner as swap_dst_owner, jt2.jetton_master as swap_dst_token, 
               jt2.amount as swap_dst_amount, jt2.query_id as swap_dst_query_id, jt2.created_lt as swap_dst_lt
              
             from transfers jt1 
             join transfers jt2 on jt1.originated_msg_id = jt2.originated_msg_id  and jt1.msg_id != jt2.msg_id and 
               jt1.query_id = jt2.query_id and jt1.source_owner = jt2.destination_owner and jt1.jetton_master != jt2.jetton_master 
             join dex_pools_info pool on pool.address = jt1.destination_owner and 
             case 
                 when pool.sub_op is null then true 
                 else jt1.sub_op = pool.sub_op end
             and pool.type = 'in'
            )
            select swap_time, 
            swap_src_owner,  swap_src_token, swap_src_amount, 
            swap_dst_owner,  swap_dst_token, swap_dst_amount
            from swaps 
            """,
            """
            refresh materialized view mview_dex_swaps;                    
            """
        ]
    )

    refresh_current_balances = PostgresOperator(
        task_id="refresh_current_balances",
        postgres_conn_id="ton_db",
        sql=[
            """
            create materialized view if not exists mview_jetton_balances
            as
            with mapping as (
              select distinct "owner", address as  wallet_address from jetton_wallets
            ),
            balances as (
              select jw."owner", jw.address as wallet_address, jw.balance, last_tx_lt, rank() over(partition by owner, jw.address order by last_tx_lt desc) as balance_rank from jetton_wallets jw
              join account_state using(state_id)
            ),
            latest as (
              select * from balances where balance_rank = 1
            ), transfer_out as (
              select latest.owner, jt.source_wallet as wallet_address,  m.created_lt, -1 * amount as delta from jetton_transfers jt 
              join messages m on jt.msg_id  = m.msg_id 
              join latest on latest.wallet_address = jt.source_wallet
              where m.created_lt  > latest.last_tx_lt and jt.successful  = true
            ), transfer_in as (
              select latest.owner, mapping.wallet_address,  m.created_lt, amount as delta from jetton_transfers jt 
              join messages m on jt.msg_id  = m.msg_id 
              join jetton_wallets src_wallet on src_wallet.address = jt.source_wallet
              join mapping on mapping.owner = jt.destination_owner
              join jetton_wallets dst_wallet on dst_wallet.address = mapping.wallet_address and src_wallet.jetton_master = dst_wallet.jetton_master 
              join latest on latest.wallet_address = dst_wallet.address
              where m.created_lt  > latest.last_tx_lt and jt.successful  = true
            ), mint as (
               select latest.owner, latest.wallet_address, m.created_lt, amount as delta  from jetton_mint jm 
               join latest on latest.wallet_address = jm.wallet
               join messages m on jm.msg_id  = m.msg_id
               where m.created_lt  > latest.last_tx_lt and jm.successful = true
            ), burn as (
               select latest.owner, latest.wallet_address, m.created_lt, -1 * amount as delta  from jetton_burn jb
               join latest on latest.wallet_address = jb.wallet
               join messages m on jb.msg_id  = m.msg_id
               where m.created_lt  > latest.last_tx_lt and jb.successful = true
            ), changes as (
              select owner, 'balance' as type, wallet_address,last_tx_lt as lt, balance as delta from balances
              union all
              select owner, 'transfer_in' as type, wallet_address, created_lt as lt, delta from transfer_in
              union all
              select owner, 'transfer_out' as type, wallet_address, created_lt as lt, delta from transfer_out
              union all
              select owner, 'mint' as type, wallet_address, created_lt as lt, delta from mint
              union all
              select owner, 'burn' as type, wallet_address, created_lt as lt, delta from burn
            )  
            select owner, wallet_address, sum(delta) as balance from changes
            group by 1, 2    
            """,
            """
            refresh materialized view mview_jetton_balances;                    
            """
        ]
    )

    add_current_top_jettons = PostgresOperator(
        task_id="add_current_top_jettons",
        postgres_conn_id="ton_db",
        sql=[
            """
        insert into top_jettons_datamart(build_time, address, 
          creation_time, symbol, price, market_volume_ton,
          market_volume_rank, active_owners_24, total_holders           
        )
 
        with enriched as ( -- add jetton symbol
          select swaps.*, jm_src.symbol as src, jm_dst.symbol as dst from mview_dex_swaps swaps
          join jetton_master jm_src on jm_src.address  = swaps.swap_src_token
          join jetton_master jm_dst on jm_dst.address  = swaps.swap_dst_token
          where swap_time  > now() - interval '1 day'
        ), trades_in_ton as ( -- only DeDust swaps for now
        select
          swap_time, swap_src_owner as swap_owner, swap_src_token as token,
          swap_src_amount as amount_token, swap_dst_amount as amount_ton,
          'sell' as direction
          from enriched where dst = 'JTON'
        union all
        select
          swap_time, swap_src_owner as swap_owner, swap_dst_token as token,
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
        ), datamart as (
          select mv.*, jm.symbol, case
            when coalesce(jm.decimals, 9) = 9 then price_raw
            when jm.decimals < 9 then price_raw / (pow(10, 9 - jm.decimals))
            else price_raw * (pow(10, 9 - jm.decimals))
          end as price, jm.decimals  from market_volume_rank as mv
          join jetton_master jm on jm.address  = mv.token 
          join prices on prices.token = mv.token
          where market_volume_rank > 100 or market_volume_ton > 10
        ), target_tokens as (
          select distinct token as address from datamart
		), min_data as (
          select address as token, to_timestamp(min(t.utime)) as creation_time
          from jetton_master jm   
          join target_tokens using(address)
          join messages m on m.destination  = jm.address  
          join transactions t on t.tx_id = m.out_tx_id 
          group by 1), 
        total_holders as (
          select target_tokens.address as token, count(distinct jw."owner") as total_holders from mview_jetton_balances b
          join jetton_wallets jw on jw.address = b.wallet_address
          join target_tokens on target_tokens.address = jw.jetton_master
          where b.balance > 0
		  group by 1
        ), active_owners as (
          select  target_tokens.address as token, count(distinct jetton_owner) as active_owners_24 from jetton_transfers jt
          join jetton_wallets jw on jw.address = jt.source_wallet
          join target_tokens on target_tokens.address = jw.jetton_master
          join messages m using(msg_id)
          join transactions tx on tx.tx_id = m.out_tx_id
          cross join unnest(array[jt.source_owner, jt.destination_owner]) as t(jetton_owner)
          where to_timestamp(tx.utime) > now() - interval '1 day' and jt.successful = true
          group by 1
        )
        select  now() as build_time, token as address, 
          md.creation_time, symbol, price,  
          market_volume_ton, market_volume_rank, 
          ao.active_owners_24, th.total_holders   
        from datamart 
        join min_data md using(token)
        join total_holders th using(token)
        join active_owners ao using(token)                         
            """
        ]
    )

    create_tables >>  refresh_dex_swaps >> refresh_current_balances >> add_current_top_jettons



rebuild_top_jettons_datamart_dag = rebuild_top_jettons_datamart()
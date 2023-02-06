#!/usr/bin/env python

from fastapi import FastAPI
import psycopg2
import psycopg2.extras
from dataclasses import dataclass

app = FastAPI()
conn = psycopg2.connect()

@dataclass
class ValueWithTrend:
    value: float
    percent: int

    @classmethod
    def create(cls, latest, prev):
        percent = None
        if prev > 0:
            percent = round(100 * (latest - prev) / prev)
        return ValueWithTrend(value=latest, percent=percent)

@dataclass
class TokenInfo:
    name: str
    price: ValueWithTrend
    marketVolume: ValueWithTrend
    totalHolders: ValueWithTrend
    activeOwners24h: ValueWithTrend
    sinceCreationSeconds: int

    @classmethod
    def from_db_row(cls, item):
        return TokenInfo(
            name=item['name'],
            price=ValueWithTrend.create(item['price'], item['prev_price']),
            marketVolume=ValueWithTrend.create(item['market_volume_ton'], item['market_volume_ton_prev']),
            totalHolders=ValueWithTrend.create(item['total_holders'], item['total_holders_prev']),
            activeOwners24h=ValueWithTrend.create(item['active_owners_24'], item['active_owners_24_prev']),
            sinceCreationSeconds=item['since_creation']
        )


@app.get("/v1/jettons/top")
async def root():
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute("""
        with latest as (
          select dm.*
                from top_jettons_datamart dm
                where build_time  = (select max(build_time) from top_jettons_datamart)
        ), prev as (
          select dm.*
                from top_jettons_datamart dm
                where build_time  = (select max(build_time) from top_jettons_datamart where build_time < now() - interval '8 hour')
        )
        select address, latest.symbol as name, latest.price, prev.price as prev_price,
        latest.market_volume_ton, prev.market_volume_ton as market_volume_ton_prev, 
        latest.total_holders, prev.total_holders as total_holders_prev,
        latest.active_owners_24, prev.active_owners_24 as active_owners_24_prev, 
        latest.creation_time, round(extract(epoch from now() - latest.creation_time)) since_creation
        from latest
        left join prev using(address)
        left join jetton_master jm using(address)
        order by latest.market_volume_rank asc limit 10
        """)
        return list(map(TokenInfo.from_db_row, cursor.fetchall()))
    finally:
        cursor.close()
#!/usr/bin/env python
import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
import aiohttp
import psycopg2
import psycopg2.extras
from dataclasses import dataclass
from urllib.parse import urlparse
import codecs
import hashlib
import os
import decimal

app = FastAPI()
conn = psycopg2.connect()
CACHE_PREFIX = os.environ.get("API_CACHE_DIR", "/tmp/")

IPFS_GATEWAY = 'https://w3s.link/ipfs/'

@dataclass
class ValueWithTrend:
    value: float
    percent: int

    @classmethod
    def create(cls, latest, prev):
        percent = None
        if prev is not None and prev > 0:
            percent = round(decimal.Decimal(100.0) * (latest - prev) / prev)
        return ValueWithTrend(value=latest, percent=percent)

@dataclass
class TokenInfo:
    name: str
    address: str
    price: ValueWithTrend
    marketVolume: ValueWithTrend
    totalHolders: ValueWithTrend
    activeOwners24h: ValueWithTrend
    sinceCreationSeconds: int

    @classmethod
    def from_db_row(cls, item):
        return TokenInfo(
            name=item['name'],
            address=item['address'],
            price=ValueWithTrend.create(item['price'], item['prev_price']),
            marketVolume=ValueWithTrend.create(item['market_volume_ton'], item['market_volume_ton_prev']),
            totalHolders=ValueWithTrend.create(item['total_holders'], item['total_holders_prev']),
            activeOwners24h=ValueWithTrend.create(item['active_owners_24'], item['active_owners_24_prev']),
            sinceCreationSeconds=item['since_creation']
        )


@app.get("/v1/jettons/top")
async def jettons():
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
                where build_time  = (select max(build_time) from top_jettons_datamart where build_time < now() - interval '24 hour')
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

async def fetch_url(url):
    parsed_url = urlparse(url)
    if parsed_url.scheme == 'ipfs':
        assert len(parsed_url.path) == 0, parsed_url
        url = IPFS_GATEWAY + parsed_url.netloc
    print(f"fetching {url}")

    async with aiohttp.ClientSession() as client:
        async with client.get(url) as resp:
            assert resp.status == 200
            return await resp.read()

def cache_key(address):
    return CACHE_PREFIX + hashlib.sha256(codecs.encode(address, "utf-8")).hexdigest()

def add_to_cache(address, content):
    key = cache_key(address)
    with open(key, "wb") as out:
        out.write(content)

def from_cache(address):
    key = cache_key(address)
    try:
        with open(key, "rb") as src:
            return src.read()
    except FileNotFoundError:
        return None

@app.get("/v1/jettons/image/{address}", response_class=Response)
async def image(address):
    cached = from_cache(address)
    if cached:
        return Response(content=cached, media_type="image/png")
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("select image, image_data from jetton_master where address  =  %s", (address,))
        res = cursor.fetchone()
        if res is None:
            raise HTTPException(status_code=404, detail="Address not found")
        if res['image_data']:
            content = codecs.decode(codecs.encode(res['image_data'], "utf-8"), "base64")
            add_to_cache(address, content)
            return Response(content=content,
                            media_type="image/png")
        if res['image']:
            content = await fetch_url(res['image'])
            add_to_cache(address, content)
            return Response(content=content, media_type="image/png")
        raise HTTPException(status_code=404, detail="No image provided for address")
    finally:
        cursor.close()

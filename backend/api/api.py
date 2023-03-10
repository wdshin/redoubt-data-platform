#!/usr/bin/env python
import os
from typing import Optional, Tuple, List, Dict

from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
import magic
import aiohttp
import psycopg2
import psycopg2.extras
from dataclasses import dataclass
from urllib.parse import urlparse
import codecs
import hashlib
import os
import decimal
from time import time
from loguru import logger


app = FastAPI(
    title="re:doubt Public API",
    version="0.2.0",
    description="""
[re:doubt](https://redoubt.online/) Public API contains a bunch of useful methods related to TON blockchain high-level information.


API is free but simple authorisation is required to access the API. To get API key send ``/start`` command
to [@RedoubtAPIBot](https://t.me/RedoubtAPIBot). API key has to be passed in ``X-API-Key`` HTTP header .For now there are no rate-limits but it could be changed 
in the near future.    
    """,
    openapi_tags=[
        {
            "name": "DEX",
            "description": """
Endpoint related to DEXs. Supported DEXs:
* [Megaton](https://megaton.fi/)
* [ston.fi](https://ston.fi/)
* [DeDust](https://dedust.io/dex/swap)
* [Tegro](https://tegro.finance/)
* [Tonswap](https://tonswap.org/swap/tokens)
            """
        }
    ]
)
origins = [
    "http://localhost:3006",
    "http://localhost:3000",
    "http://beta.redoubt.online",
    "http://redoubt.online",
    "https://redoubt.online",
    "https://www.redoubt.online",
    "https://app.redoubt.online",
    "http://beta-app.redoubt.online"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)
CACHE_PREFIX = os.environ.get("API_CACHE_DIR", "/tmp/")
NO_AUTH_MODE = bool(os.environ.get("NO_AUTH_MODE", "false"))

IPFS_GATEWAY = 'https://w3s.link/ipfs/'

@dataclass
class ValueWithTrend:
    value: float
    percent: Optional[int]

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
    # sinceCreationSeconds: int

    @classmethod
    def from_db_row(cls, item):
        return TokenInfo(
            name=item['name'],
            address=item['address'],
            price=ValueWithTrend.create(item['price'], item['prev_price']),
            marketVolume=ValueWithTrend.create(item['market_volume_ton'], item['market_volume_ton_prev']),
            totalHolders=ValueWithTrend.create(item['total_holders'], item['total_holders_prev']),
            activeOwners24h=ValueWithTrend.create(item['active_owners_24'], item['active_owners_24_prev'])
            # sinceCreationSeconds=item['since_creation']
        )

@dataclass
class PlatformInfo:
    name: str
    marketVolume: int


    @classmethod
    def from_db_row(cls, item):
        return PlatformInfo(
            name=item['platform'],
            marketVolume=item['market_volume_ton'])


@dataclass
class TopJettonsInfo:
    jettons: list[TokenInfo]
    platforms: list[PlatformInfo]
    total: int

MIN_MARKET_VOLUME = 300

@app.get("/v1/jettons/top", response_model=TopJettonsInfo, tags=["jettons"])
async def jettons() -> TopJettonsInfo:
    """
    Returns overall stats for top jettons and exchanges (both DEXs and CEXs). Only jettons 
    with at least 300 TON 24h market volume included in the list.
    """
    conn = psycopg2.connect()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute(f"""
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
        where latest.market_volume_ton > {MIN_MARKET_VOLUME} or latest.market_volume_rank  < 10
        order by latest.market_volume_rank asc -- limit 10
        """)
        jettons = list(map(TokenInfo.from_db_row, cursor.fetchall()))

        cursor.execute("""
        select dm.*
                from platform_volume_24_datamart dm
                where build_time  = (select max(build_time) from platform_volume_24_datamart)
        """)
        platforms = list(map(PlatformInfo.from_db_row, cursor.fetchall()))

        cursor
        return TopJettonsInfo(
            jettons=jettons,
            platforms=platforms,
            total=sum(map(lambda x: x.marketVolume, platforms))
        )
    finally:
        cursor.close()
        conn.close

async def fetch_url(url):
    parsed_url = urlparse(url)
    if parsed_url.scheme == 'ipfs':
        assert len(parsed_url.path) == 0, parsed_url
        url = IPFS_GATEWAY + parsed_url.netloc
    print(f"fetching {url}")

    async with aiohttp.ClientSession(headers={'User-Agent': 'Re:doubt API indexer'}) as client:
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
    print(key)
    try:
        with open(key, "rb") as src:
            return src.read()
    except FileNotFoundError:
        return None

def with_content_type(content):
    mime = magic.from_buffer(content, mime=True)
    print(mime)
    return Response(content=content, media_type=mime, headers={
        'Cache-Control': 'public, max-age=86400'
    })

@app.get("/v1/jettons/image/{address}", response_class=Response, tags=["jettons"])
async def image(address):
    """
    Returns jetton image as binary content.
    """
    cached = from_cache(address)
    if cached:
        return with_content_type(cached)
    try:
        conn = psycopg2.connect()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("select image, image_data from jetton_master where address  =  %s", (address,))
        res = cursor.fetchone()
        if res is None:
            raise HTTPException(status_code=404, detail="Address not found")
        if res['image_data']:
            content = codecs.decode(codecs.encode(res['image_data'], "utf-8"), "base64")
            add_to_cache(address, content)
            return with_content_type(content)
        if res['image']:
            content = await fetch_url(res['image'])
            add_to_cache(address, content)
            return with_content_type(content)
        raise HTTPException(status_code=404, detail="No image provided for address")
    finally:
        cursor.close()
        conn.close()


API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

api_conn = psycopg2.connect()
api_cursor = api_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def track_access(api_user: APIKey, method: str, arg0: str=None):
    if api_user is None:
        return
    logger.info(f"Tracking method {method} call from {api_user}")
    api_cursor.execute("""
    insert into api_access_log(call_time, user_id, method_name, arg0)
    values (now(), %s, %s, %s)
    """, (api_user, method, arg0))
    api_conn.commit()

async def get_api_key(api_key_header: str = Security(api_key_header)):
    # for debug purposes onlu
    if NO_AUTH_MODE:
        return None
    # logger.info(api_key_header)
    api_cursor.execute("select * from api_keys where api_key = %s", (api_key_header,))
    res = api_cursor.fetchone()
    if not res:
        raise HTTPException(status_code=403, detail="X-API-Key header is missing or wrong")

    return res['user_id']


@app.get("/v1/dex", tags=["DEX"], response_model=list[PlatformInfo])
async def dexs(api_user: APIKey = Depends(get_api_key)):
    """
    Returns basic information about DEXs: market volume and TVL.
    """
    track_access(api_user, "/v1/dex")
    api_cursor.execute("""
    select dm.*
            from platform_volume_24_datamart dm
            where build_time  = (select max(build_time) from platform_volume_24_datamart)
    """)
    return list(map(PlatformInfo.from_db_row, api_cursor.fetchall()))

@dataclass
class PricePoint:
    ts: int
    price: float
    volume: int


@app.get("/v1/jettons/{address}/price/history", tags=["jettons"], response_model=list[PricePoint])
async def price_history(address: str, start_time: Optional[int] = None, end_time: Optional[int] = None,
                        api_user: APIKey = Depends(get_api_key)) -> list[PricePoint]:
    """
    Returns price history for Jetton identified by {address} param. Each price point consist of
    timestamp, price value and 24h volume in TON. ``start_time`` and ``end_time`` query params
    could be passed to specify interval. ``start_time`` must be less than ``end_time`` (``end_time`` represents
    end of the interval). Maximum allowed interval length is 7 days. Default value for ``end_time``
    is current time, default value for ``start_time`` is 7 days before ``end_time``.

    Prices and market volumes returned averaged with 1 hour window.
    """

    WEEK = 86400 * 7

    track_access(api_user, "/v1/jettons/{address}/price/history", address)
    logger.info(f"History for {address} {start_time} {end_time}")
    if end_time is None:
        end_time = int(time())
    if start_time is None:
        start_time = end_time - WEEK
    if end_time < start_time:
        raise HTTPException(status_code=400, detail="start_time must be < end_time")
    if (end_time - start_time) / 86400 > 7:
        raise HTTPException(status_code=400, detail="Interval between start_time and end_time is too large")
    api_cursor.execute("""
        select extract(epoch from date_trunc('hour', build_time)) as ts, 
        avg(price) as price, round(avg(market_volume_ton)) as volume 
        from top_jettons_datamart tjd 
        where address =%s
        and build_time <= to_timestamp(%s) and build_time > to_timestamp(%s)
        group by 1 
        order by 1 desc
    """, (address, end_time, start_time))
    items = []
    for row in api_cursor.fetchall():
        items.append(PricePoint(ts=row['ts'], price=row['price'], volume=row['volume']))
    return items
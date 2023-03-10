#!/usr/bin/env python
import os

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



@app.get("/v1/jettons/top")
async def jettons():
    conn = psycopg2.connect()
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
        where latest.market_volume_ton > 300 or latest.market_volume_rank  < 10
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
        return {
            'jettons': jettons,
            'platforms': platforms,
            'total': sum(map(lambda x: x.marketVolume, platforms))
        }
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

@app.get("/v1/jettons/image/{address}", response_class=Response)
async def image(address):
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

def track_access(api_user: APIKey, method: str):
    logger.info(f"Tracking method {method} call from {api_user}")
    api_cursor.execute("""
    insert into api_access_log(call_time, user_id, method_name)
    values (now(), %s, %s)
    """, (api_user, method))
    api_conn.commit()

async def get_api_key(api_key_header: str = Security(api_key_header)):
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
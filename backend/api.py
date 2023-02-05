#!/usr/bin/env python

from fastapi import FastAPI
import psycopg2
import logging

app = FastAPI()
conn = psycopg2.connect()

@app.get("/api/v1/jettons/top")
async def root():
    with conn.cursor() as cursor:
        cursor.execute("""
        select dm.* 
        from top_jettons_datamart dm
        where build_time  = (select max(build_time) from top_jettons_datamart) order by market_volume_rank asc
        """)
        return cursor.fetchall()
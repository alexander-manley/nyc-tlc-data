from fastapi import FastAPI

import pandas

from sqlalchemy import create_engine

db_connection_url = "postgresql://docker:12345@localhost:5432/nyc_tlc"
engine = create_engine(db_connection_url)

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/api/tip/2020/{quarter}/max")
async def max_tip(quarter: str):
    quarter = int(quarter.lstrip('0'))
    df = pandas.read_sql_table("highest_tip_perc", engine)
    mq = df.groupby(['quarter'])[['quarter', 'maxTipPercentage']].max()
    mq = mq[mq['quarter'] == quarter]
    max_quarter = mq['maxTipPercentage'].iloc[0]
    res = {"maxTipPercentage": max_quarter}
    return res

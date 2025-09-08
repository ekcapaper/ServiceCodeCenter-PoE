import asyncio
import logging
import random
from contextlib import asynccontextmanager, suppress
from typing import AsyncGenerator

import uvicorn
from fakeredis import FakeAsyncRedis
from fastapi import FastAPI
from prefect import flow, task

log = logging.getLogger(__name__)

redis_client = FakeAsyncRedis()


# 데이터 수집 작업
@task(name="task_raw_data_main")
async def task_raw_data_main():
    raw = random.randint(0, 100)
    await redis_client.set("raw_data", raw)


result_bool = False


# 데이터 분석 작업
@task(name="task_data_analysis")
async def task_data_analysis():
    global result_bool
    raw = int(await redis_client.get("raw_data"))
    if raw > 50 and not result_bool:
        result_bool = True
    if raw < 50 and result_bool:
        result_bool = False
        cur = int(await redis_client.get("current_delivery"))
        await redis_client.set("delivery", cur + 1)


# 실행을 도와주는 함수
@flow(name="runner")
async def runner():
    while True:
        await task_raw_data_main()
        await task_data_analysis()
        await asyncio.sleep(1)


# lifespan
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await redis_client.set("raw_data", 0)
    await redis_client.set("current_delivery", 0)
    task1 = asyncio.create_task(runner())
    try:
        yield
    finally:
        task1.cancel()
        with suppress(asyncio.CancelledError):
            await task1


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/raw-data")
async def get_raw_data():
    return {"raw_data": int(await redis_client.get("raw_data"))}


@app.get("/delivery")
async def get_delivery():
    return {"delivery": int(await redis_client.get("current_delivery"))}


@app.get("/node-graph")
def get_node_graph():
    return {
        "nodes": [
            {"id": "frontend", "title": "Frontend", "subTitle": "Service"},
            {"id": "backend", "title": "Backend", "subTitle": "Service"}
        ],
        "edges": [
            {"id": "1", "source": "frontend", "target": "backend", "mainStat": "120 req/s"}
        ]
    }


if __name__ == '__main__':
    # uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

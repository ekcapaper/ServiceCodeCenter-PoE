import asyncio
import logging
import random
import time
from contextlib import asynccontextmanager, suppress
from typing import AsyncGenerator

import uvicorn
from fakeredis import FakeAsyncRedis
from fastapi import FastAPI
from prefect import flow, task

log = logging.getLogger(__name__)

redis_client = FakeAsyncRedis()


# 데이터 수집 작업
#@task(name="task_raw_data_main")
async def task_raw_data_main():
    raw = random.randint(0, 100)
    await redis_client.set("raw_data", raw)


result_bool = False


# 데이터 분석 작업
#@task(name="task_data_analysis")
async def task_data_analysis():
    global result_bool
    raw = int(await redis_client.get("raw_data"))
    if raw > 50 and not result_bool:
        result_bool = True
    if raw < 50 and result_bool:
        result_bool = False
        cur = int(await redis_client.get("current_delivery"))
        await redis_client.set("current_delivery", cur + 1)


# 실행을 도와주는 함수
#@flow(name="runner")
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

# data
@app.get("/raw-data")
async def get_raw_data():
    now_ms = int(time.time() * 1000)
    return [
      {
        "target": "raw-data",
        "datapoints": [
          [int(await redis_client.get("raw_data")), now_ms]
        ]
      }
    ]

@app.get("/delivery")
async def get_delivery():
    now_ms = int(time.time() * 1000)
    return [
        {
            "target": "delivery",
            "datapoints": [
                [int(await redis_client.get("current_delivery")), now_ms]
            ]
        }
    ]


# node-graph
@app.get("/api/graph/fields")
def get_node_graph():
    return {
        "edges_fields": [
            {
                "field_name": "id",
                "type": "string"
            },
            {
                "field_name": "source",
                "type": "string"
            },
            {
                "field_name": "target",
                "type": "string"
            }
        ],
        "nodes_fields": [
            {
                "field_name": "id",
                "type": "string"
            },
            {
                "field_name": "title",
                "type": "string"
            }
        ]
    }


@app.get("/api/graph/data")
def get_graph_data():
    return {
        "edges": [
            {
                "id": "1",
                "source": "1",
                "target": "2"
            }
        ],
        "nodes": [
            {
                "id": "1",
                "title": "Service1"
            },
            {
                "id": "2",
                "title": "Service2"
            }
        ]
    }

@app.get("/api/health")
def get_health():
    return {}

if __name__ == '__main__':
    # uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

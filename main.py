import random
from contextlib import asynccontextmanager, suppress
from threading import Thread
from typing import AsyncGenerator

from fakeredis import FakeAsyncRedis
from fastapi import FastAPI
import uvicorn
import asyncio
import logging

from prefect import flow, task, get_run_logger

log = logging.getLogger(__name__)

redis_client = FakeAsyncRedis()

#@flow(name="raw_data_main")
async def raw_data_main():
    try:
        while True:
            raw_data = random.randint(0, 100)
            await redis_client.set("raw_data", raw_data)
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        log.info("data_main cancelled")
        raise
    except Exception:
        log.exception("data_main crashed")

# 전문가 시스템 가정
result_bool = False
async def data_analysis():
    global result_bool
    try:
        while True:
            raw_data = int(await redis_client.get("raw_data"))
            if raw_data > 70 and not result_bool:
                result_bool = True
            if raw_data < 30 and result_bool:
                result_bool = False
                current_delivery = int(await redis_client.get("current_delivery"))
                await redis_client.set("delivery", current_delivery + 1)
    except asyncio.CancelledError:
        log.info("data_main cancelled")
        raise
    except Exception:
        log.exception("data_main crashed")

# 학습

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await redis_client.set("raw_data", 0)
    await redis_client.set("current_delivery", 0)
    task1 = asyncio.create_task(raw_data_main())
    task2 = asyncio.create_task(data_analysis())
    try:
        yield
    finally:
        task1.cancel()
        task2.cancel()
        with suppress(asyncio.CancelledError):
            await task1
            await task2

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/raw-data")
async def raw_data():
    return {"raw_data": int(redis_client.get("raw_data"))}

@app.get("/delivery")
async def delivery():
    return {"delivery": int(redis_client.get("delivery"))}

if __name__ == '__main__':
    # uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

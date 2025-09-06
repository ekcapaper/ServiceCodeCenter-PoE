import random
from contextlib import asynccontextmanager, suppress
from threading import Thread
from typing import AsyncGenerator

from fakeredis import FakeAsyncRedis
from fastapi import FastAPI
import uvicorn
import asyncio
import logging

log = logging.getLogger(__name__)

redis_client = FakeAsyncRedis()

@flow(name="data-loop")
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
            raw_data = await redis_client.get("raw_data")
            if raw_data > 70 and not result_bool:
                result_bool = True
            if raw_data < 30 and result_bool:
                result_bool = False


    except asyncio.CancelledError:
        log.info("data_main cancelled")
        raise
    except Exception:
        log.exception("data_main crashed")

# 학습

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await redis_client.set("raw_data", 0)
    await redis_client.set("delivery", 0)
    task = asyncio.create_task(raw_data_main())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/raw-data")
async def raw_data():
    return {"raw_data": int(redis_client.get("raw_data"))}

if __name__ == '__main__':
    # uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

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
@task(name="task_raw_data_main")
async def task_raw_data_main():
    raw = random.randint(0, 100)
    await redis_client.set("raw_data", raw)

result_bool = False

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

@flow(name="runner")
async def runner():
    while True:
        await task_raw_data_main()
        await task_data_analysis()
        await asyncio.sleep(1)

# 학습

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

if __name__ == '__main__':
    # uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

from contextlib import asynccontextmanager, suppress
from threading import Thread
from typing import AsyncGenerator

import fakeredis
from fastapi import FastAPI
import uvicorn
import asyncio
import logging

log = logging.getLogger(__name__)

redis_client = fakeredis.FakeRedis()
redis_client.set("node_a", 0)

async def data_main():
    try:
        while True:
            data = redis_client.get("node_a")
            data_int = int(data)
            print(data)
            redis_client.set("node_a", data_int + 1)
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        log.info("data_main cancelled")
        raise
    except Exception:
        log.exception("data_main crashed")

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    task = asyncio.create_task(data_main())
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

if __name__ == '__main__':
    # uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

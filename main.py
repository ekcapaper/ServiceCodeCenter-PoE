from contextlib import asynccontextmanager, suppress
from typing import AsyncGenerator

import redis
from fastapi import FastAPI
import uvicorn
import asyncio
import logging

from fakeredis import TcpFakeServer
import fakeredis

server_address = ("127.0.0.1", 6379)
server = TcpFakeServer(server_address, server_type="redis")


log = logging.getLogger(__name__)

async def data_main():
    try:
        while True:
            r = redis.Redis(host=server_address[0], port=server_address[1])
            await r.set("foo", "bar")
            print(await r.get("foo"))
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
    uvicorn.run(app, host="0.0.0.0", port=8000)

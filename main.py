from contextlib import asynccontextmanager, suppress
from typing import AsyncGenerator

from fastapi import FastAPI
import uvicorn
import asyncio
import logging

log = logging.getLogger(__name__)

async def data_main():
    try:
        while True:
            print("temp")  # 실서비스에선 logging 사용 권장
            await asyncio.sleep(1)  # 취소 지점(cancellation point)
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

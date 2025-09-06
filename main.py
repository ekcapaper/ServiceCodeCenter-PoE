from contextlib import contextmanager, asynccontextmanager
from typing import Generator, Any, AsyncGenerator

from fastapi import FastAPI, HTTPException
import uvicorn
import asyncio

async def data_main():
    while True:
        print("temp")
        await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    asyncio.create_task(data_main())
    yield

app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"message": "Hello World"}




if __name__ == '__main__':
    uvicorn.run(app)


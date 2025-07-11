import redis.asyncio as redis
from dotenv import load_dotenv
import os

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost")
print("REDIS_URL: ",REDIS_URL)
redis_client = redis.from_url(REDIS_URL)

async def get_redis():
    return redis_client

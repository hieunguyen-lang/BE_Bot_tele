from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

# MySQL connection URL format: mysql+aiomysql://user:password@host:port/database
SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL")

# Create async engined
engine = create_async_engine(
    SQLALCHEMY_DATABASE_URL,
    echo=True,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,  # Recycle connections after 30 minutes
)

# Create async session factory
async_session = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

# Dependency
async def get_db():
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close() 
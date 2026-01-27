"""
MongoDB database configuration using Motor and Beanie.
"""
import os
from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie

from app.models import ALL_MODELS

# MongoDB configuration
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
DATABASE_NAME = os.getenv("DATABASE_NAME", "celery_aio_demo")

# Global client instance
_client: AsyncIOMotorClient | None = None


async def init_db():
    """
    Initialize the database connection and Beanie ODM.
    Call this on application startup.
    """
    global _client
    
    _client = AsyncIOMotorClient(MONGODB_URL)
    
    await init_beanie(
        database=_client[DATABASE_NAME],
        document_models=ALL_MODELS,
    )
    
    print(f"Connected to MongoDB: {MONGODB_URL}/{DATABASE_NAME}")
    return _client


async def close_db():
    """
    Close the database connection.
    Call this on application shutdown.
    """
    global _client
    if _client:
        _client.close()
        _client = None
        print("MongoDB connection closed")


def get_client() -> AsyncIOMotorClient:
    """Get the MongoDB client instance."""
    if _client is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _client


async def get_db_for_celery():
    """
    Get or initialize database connection for Celery tasks.
    
    Celery workers need to initialize their own connection
    since they run in separate processes.
    """
    global _client
    
    if _client is None:
        await init_db()
    
    return _client

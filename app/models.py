"""
Beanie ODM models for MongoDB.

These models represent documents stored in MongoDB and can be
used with async operations - perfect for celery-aio-pool tasks.
"""
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from beanie import Document, Indexed
from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Status of a background job."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class JobResult(BaseModel):
    """Result data for a completed job."""
    data: dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None
    execution_time_ms: Optional[float] = None


class Job(Document):
    """
    Job document - tracks background task execution.
    
    This is stored in MongoDB and can be queried/updated
    asynchronously from Celery tasks.
    """
    # Indexed fields for fast queries
    celery_task_id: Indexed(str, unique=True)
    job_type: Indexed(str)
    status: Indexed(str) = JobStatus.PENDING.value
    
    # Job metadata
    name: str
    description: Optional[str] = None
    priority: int = Field(default=0, ge=0, le=10)
    
    # Input/Output
    input_data: dict[str, Any] = Field(default_factory=dict)
    result: Optional[JobResult] = None
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Retry information
    retry_count: int = 0
    max_retries: int = 3
    
    class Settings:
        name = "jobs"
        use_state_management = True
    
    @property
    def duration_ms(self) -> Optional[float]:
        """Calculate job duration in milliseconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds() * 1000
        return None


class User(Document):
    """
    User document - example of a typical application model.
    """
    email: Indexed(str, unique=True)
    username: Indexed(str, unique=True)
    full_name: str
    
    # Profile data
    bio: Optional[str] = None
    avatar_url: Optional[str] = None
    
    # Status
    is_active: bool = True
    is_verified: bool = False
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_login: Optional[datetime] = None
    
    # Stats (can be updated by background tasks)
    login_count: int = 0
    task_count: int = 0
    
    class Settings:
        name = "users"


class DataRecord(Document):
    """
    Generic data record - for storing processed data.
    
    Example use case: A background task fetches data from an API,
    processes it, and stores the result here.
    """
    source: Indexed(str)  # Where the data came from
    record_type: Indexed(str)  # Type of record
    
    # The actual data
    data: dict[str, Any] = Field(default_factory=dict)
    
    # Metadata
    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    processed_at: Optional[datetime] = None
    
    # Processing info
    processing_task_id: Optional[str] = None
    is_processed: bool = False
    
    class Settings:
        name = "data_records"


class APICallLog(Document):
    """
    Log of API calls made by background tasks.
    
    Useful for auditing, debugging, and rate limit tracking.
    """
    task_id: Indexed(str)
    url: str
    method: str = "GET"
    
    # Request info
    request_headers: dict[str, str] = Field(default_factory=dict)
    request_body: Optional[str] = None
    
    # Response info
    status_code: Optional[int] = None
    response_time_ms: Optional[float] = None
    response_size_bytes: Optional[int] = None
    
    # Error info
    error: Optional[str] = None
    
    # Timestamp
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Settings:
        name = "api_call_logs"


# List of all document models for Beanie initialization
ALL_MODELS = [Job, User, DataRecord, APICallLog]

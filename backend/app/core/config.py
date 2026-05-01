"""
Centralized configuration loaded from environment variables.
Everything tunable lives here so we never sprinkle magic numbers in business logic.
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # --- Service identity ---
    APP_NAME: str = "ITSS"
    ENV: str = "development"

    # --- Redis (queue + ephemeral state) ---
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_QUEUE_KEY: str = "itss:worker_queue"          # FIFO list workers BLPOP from
    REDIS_RUNNING_SET: str = "itss:running"             # SET of currently-running task IDs (dedup)
    REDIS_RESULT_CHANNEL: str = "itss:results"          # pub/sub for live updates
    REDIS_HEARTBEAT_PREFIX: str = "itss:hb:"            # worker heartbeat keys w/ TTL

    # --- Database (durable state) ---
    DATABASE_URL: str = "postgresql+asyncpg://itss:itss@localhost:5432/itss"

    # --- Scheduler tuning ---
    SCHEDULER_TICK_SECONDS: float = 0.5                 # how often the heap is checked
    MAX_RETRIES: int = 5
    BASE_BACKOFF_SECONDS: float = 2.0                   # exponential base: 2,4,8,16,32...
    MAX_BACKOFF_SECONDS: float = 600.0                  # cap so we don't wait days
    WORKER_HEARTBEAT_TTL: int = 30                      # seconds before worker considered dead

    # --- ML ---
    ML_MODEL_PATH: str = "/app/ml/models/priority_model.joblib"
    ML_ENABLED: bool = True

    class Config:
        env_file = ".env"


settings = Settings()

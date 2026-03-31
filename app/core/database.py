"""PostgreSQL connectivity using SQLAlchemy and environment-based configuration."""

from __future__ import annotations

import os
from contextlib import contextmanager
from functools import lru_cache
from typing import Iterator, Any
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

load_dotenv()


def get_database_url() -> str:
    """
    Return a SQLAlchemy database URL.

    Uses ``DATABASE_URL`` when set. Otherwise builds a URL from ``POSTGRES_*``
    variables (see ``.env.example``).
    """
    url = os.getenv("DATABASE_URL", "").strip()
    if url:
        return url

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = quote_plus(os.getenv("POSTGRES_USER", "postgres"))
    password = quote_plus(os.getenv("POSTGRES_PASSWORD", ""))
    db = os.getenv("POSTGRES_DB", "postgres")

    auth = f"{user}:{password}" if password else user
    return f"postgresql+psycopg2://{auth}@{host}:{port}/{db}"


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """
    Return a cached SQLAlchemy Engine instance.

    This function creates the database engine once and reuses it across the
    application lifecycle to avoid unnecessary connection overhead.

    Notes
    -----
    - The engine is configured using environment variables.
    - SQL query logging (echo) is controlled via the `SQLALCHEMY_ECHO` env var.
      Set `SQLALCHEMY_ECHO=true` to enable SQL logging for debugging.
    - Uses connection pooling with `pool_pre_ping=True` to automatically
      validate connections before use, preventing stale connection errors.
    """

    # Read SQL echo setting from environment (defaults to False)
    echo = os.getenv("SQLALCHEMY_ECHO", "false").lower() == "true"

    # Create and return the SQLAlchemy engine
    return create_engine(
        get_database_url(),
        echo=echo,
        pool_pre_ping=True,
    )

@contextmanager
def get_connection() -> Iterator[Connection]:
    """
    Yield a SQLAlchemy connection within a transaction (commit on success).

    Suitable for ordinary SQL executed through SQLAlchemy.
    """
    engine = get_engine()
    with engine.begin() as conn:
        yield conn


@contextmanager
def get_raw_connection() -> Iterator[Any]:
    """
    Yield a DBAPI connection (psycopg2) for operations that need the native driver.

    Commits on success and rolls back on error. Use for ``COPY``, bulk loads, or
    libraries that expect a PEP-249 connection.
    """
    engine = get_engine()
    raw = engine.raw_connection()
    try:
        yield raw
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()

"""
Database connection management for historical orderbook imports.

Provides asyncpg connection pool management with appropriate configuration
for batch import operations.
"""

import asyncpg
import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
    # Load .env from project root or current directory
    env_paths = [
        Path(__file__).parent.parent / '.env',  # Project root
        Path.cwd() / '.env',                     # Current directory
    ]
    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            break
except ImportError:
    pass  # dotenv not installed, use environment variables directly


class DatabaseConnectionManager:
    """Manages asyncpg connection pool for historical imports.

    Provides connection pooling with configuration optimized for
    batch insert operations during historical data import.

    Attributes:
        pool: asyncpg connection pool instance.
        dsn: Database connection string.
        min_size: Minimum pool connections.
        max_size: Maximum pool connections.

    Example:
        >>> db_manager = DatabaseConnectionManager()
        >>> await db_manager.create_pool()
        >>> async with db_manager.acquire() as conn:
        ...     await conn.execute("SELECT 1")
        >>> await db_manager.close()
    """

    DEFAULT_DSN = "postgresql://andy_henderson@localhost:5433/FinancialDB_V2"

    def __init__(
        self,
        dsn: Optional[str] = None,
        min_size: int = 2,
        max_size: int = 10,
        command_timeout: int = 120
    ):
        """Initialize database connection manager.

        Args:
            dsn: Database connection string. If not provided, uses
                DATABASE_URL environment variable or default.
            min_size: Minimum number of connections in the pool.
            max_size: Maximum number of connections in the pool.
            command_timeout: Timeout for database commands in seconds.
        """
        self.dsn = dsn or os.environ.get('DATABASE_URL', self.DEFAULT_DSN)
        self.min_size = min_size
        self.max_size = max_size
        self.command_timeout = command_timeout
        self.pool: Optional[asyncpg.Pool] = None

    async def create_pool(self) -> asyncpg.Pool:
        """Create and return the connection pool.

        Returns:
            asyncpg.Pool: The created connection pool.

        Raises:
            asyncpg.PostgresError: If connection fails.
        """
        if self.pool is not None:
            return self.pool

        self.pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=self.min_size,
            max_size=self.max_size,
            command_timeout=self.command_timeout
        )
        return self.pool

    async def close(self) -> None:
        """Close the connection pool."""
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    def acquire(self):
        """Acquire a connection from the pool.

        Returns:
            asyncpg.pool.PoolAcquireContext: Context manager for connection.

        Raises:
            RuntimeError: If pool has not been created.

        Example:
            >>> async with db_manager.acquire() as conn:
            ...     await conn.execute("SELECT 1")
        """
        if self.pool is None:
            raise RuntimeError("Pool not created. Call create_pool() first.")
        return self.pool.acquire()

    async def __aenter__(self):
        """Async context manager entry."""
        await self.create_pool()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


async def load_symbol_mapping(conn: asyncpg.Connection) -> dict:
    """Load symbol to ID mapping from database.

    Args:
        conn: Database connection.

    Returns:
        dict: Mapping of symbol names to IDs (e.g., {'BTCUSDT': 1}).

    Example:
        >>> symbol_map = await load_symbol_mapping(conn)
        >>> symbol_map['BTCUSDT']
        1
    """
    rows = await conn.fetch(
        "SELECT id, symbol FROM symbols WHERE exchange = 'bybit'"
    )
    return {row['symbol']: row['id'] for row in rows}

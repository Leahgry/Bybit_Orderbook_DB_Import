"""
Partition management for historical orderbook imports.

Handles creation of monthly partitions for orderbook_deltas_archive
and orderbook_archive tables.
"""

import asyncpg
from datetime import date
from typing import List


class PartitionManager:
    """Manages monthly partitions for orderbook tables.

    Ensures partitions exist before data import to avoid insert failures.
    Creates partitions for both orderbook_deltas_archive and orderbook_archive
    tables on the ts_orderbook tablespace.

    Example:
        >>> partition_mgr = PartitionManager()
        >>> await partition_mgr.ensure_partition_exists(
        ...     conn, 'orderbook_deltas_archive', date(2025, 1, 15)
        ... )
    """

    TABLES = ['orderbook_deltas_archive', 'orderbook_archive']
    TABLESPACE = 'ts_orderbook'

    async def ensure_partition_exists(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        partition_date: date,
        tablespace: str = None
    ) -> str:
        """Ensure a monthly partition exists for the given date.

        Args:
            conn: Database connection.
            table_name: Name of the partitioned table.
            partition_date: Any date within the target month.
            tablespace: Tablespace for the partition.

        Returns:
            str: Name of the partition (created or existing).

        Raises:
            asyncpg.PostgresError: If partition creation fails.
        """
        tablespace = tablespace or self.TABLESPACE

        result = await conn.fetchval(
            "SELECT create_monthly_partition($1, $2, $3)",
            table_name,
            partition_date,
            tablespace
        )
        return result

    async def ensure_partitions_for_date(
        self,
        conn: asyncpg.Connection,
        target_date: date
    ) -> List[str]:
        """Ensure partitions exist for both archive tables for a date.

        Args:
            conn: Database connection.
            target_date: Date for which partitions are needed.

        Returns:
            List[str]: Names of partitions (created or existing).
        """
        partitions = []
        for table in self.TABLES:
            partition = await self.ensure_partition_exists(
                conn, table, target_date
            )
            partitions.append(partition)
        return partitions

    async def ensure_partitions_for_range(
        self,
        conn: asyncpg.Connection,
        start_date: date,
        end_date: date
    ) -> List[str]:
        """Create all partitions needed for a date range.

        Args:
            conn: Database connection.
            start_date: Start of the date range.
            end_date: End of the date range.

        Returns:
            List[str]: Names of all partitions created.
        """
        created = []
        current = start_date.replace(day=1)

        while current <= end_date:
            for table in self.TABLES:
                result = await self.ensure_partition_exists(
                    conn, table, current
                )
                created.append(result)

            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        return created

    async def check_partition_exists(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        target_date: date
    ) -> bool:
        """Check if a partition exists for the given date.

        Args:
            conn: Database connection.
            table_name: Name of the partitioned table.
            target_date: Date to check partition for.

        Returns:
            bool: True if partition exists, False otherwise.
        """
        partition_name = f"{table_name}_{target_date.strftime('%Y_%m')}"
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_class WHERE relname = $1",
            partition_name
        )
        return exists is not None

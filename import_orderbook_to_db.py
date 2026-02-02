#!/usr/bin/env python3
"""
Import Bybit orderbook data to FinancialDB_V2.

This script processes orderbook files containing snapshot and delta updates,
imports deltas to orderbook_deltas_archive, and minute snapshots to orderbook_archive.
Successfully imported files are moved to ImportedFiles directory.
"""

import asyncio
import json
import zlib
import zipfile
import gzip
import shutil
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone, date, timedelta
from collections import OrderedDict
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field

import asyncpg
import numpy as np

from db.connection import DatabaseConnectionManager, load_symbol_mapping
from db.partitions import PartitionManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SOURCE_DIR = Path("/Volumes/Backup01/FinancialData/Bybit/OrderBook")
IMPORTED_DIR = Path("/Volumes/Backup01/FinancialData/Bybit/OrderBook/ImportedFiles")
TEMP_DIR = Path("/tmp/orderbook_db_import")

BATCH_SIZE_DELTAS = 2000
BATCH_SIZE_SNAPSHOTS = 100
DEFAULT_CONCURRENCY = 4
DEPTH = 1000

# Data type IDs from data_types table
DATA_TYPE_ORDERBOOK_SNAPSHOT = 8  # 'source_orderbook'
DATA_TYPE_ORDERBOOK_DELTA = 9     # 'source_orderbook_delta'
RANGE_TYPE_RAW_DATA = 1           # 'raw_data'


@dataclass
class ImportResult:
    """Result of importing a single archive file."""

    success: bool
    file_path: Path
    symbol: str
    deltas_imported: int = 0
    snapshots_imported: int = 0
    deltas_skipped: int = 0
    error_message: str = ""
    data_start_ts: Optional[datetime] = None
    data_end_ts: Optional[datetime] = None


@dataclass
class ImportSummary:
    """Summary of a batch import operation."""

    total_files: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    total_deltas: int = 0
    total_snapshots: int = 0
    errors: List[str] = field(default_factory=list)


class OrderbookState:
    """Maintains current orderbook state by applying snapshots and deltas.

    Tracks bid and ask price levels, applying updates from snapshot and delta
    messages to reconstruct the orderbook state at any point in time.

    Attributes:
        bids: Dictionary mapping price to quantity for bid levels.
        asks: Dictionary mapping price to quantity for ask levels.
        max_depth: Maximum number of levels to track.
        last_update_id: Update ID from last applied message.
        last_timestamp: Timestamp of last applied message.
    """

    def __init__(self, max_depth: int = DEPTH):
        """Initialize orderbook state.

        Args:
            max_depth: Maximum depth of orderbook to track.
        """
        self.bids: Dict[str, str] = OrderedDict()
        self.asks: Dict[str, str] = OrderedDict()
        self.max_depth = max_depth
        self.last_update_id: Optional[int] = None
        self.last_timestamp: Optional[int] = None

    def apply_snapshot(self, data: dict, timestamp: int) -> None:
        """Apply a snapshot to reset the orderbook state.

        Args:
            data: Snapshot data containing 'b' (bids) and 'a' (asks) arrays.
            timestamp: Timestamp of the snapshot in milliseconds.
        """
        self.bids.clear()
        self.asks.clear()

        for price, qty in data.get('b', []):
            self.bids[price] = qty

        for price, qty in data.get('a', []):
            self.asks[price] = qty

        self.last_update_id = data.get('u')
        self.last_timestamp = timestamp

    def apply_delta(self, data: dict, timestamp: int) -> None:
        """Apply a delta update to modify the orderbook state.

        Args:
            data: Delta data containing 'b' (bids) and 'a' (asks) updates.
            timestamp: Timestamp of the delta in milliseconds.
        """
        for price, qty in data.get('b', []):
            if qty == '0' or float(qty) == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty

        for price, qty in data.get('a', []):
            if qty == '0' or float(qty) == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

        self.last_update_id = data.get('u')
        self.last_timestamp = timestamp

    def get_sorted_levels(self) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """Get sorted bid and ask levels.

        Returns:
            Tuple of (bids, asks) where each is a list of (price, qty) tuples.
            Bids are sorted descending (best bid first).
            Asks are sorted ascending (best ask first).
        """
        sorted_bids = sorted(
            [(float(p), float(q)) for p, q in self.bids.items()],
            key=lambda x: -x[0]
        )[:self.max_depth]

        sorted_asks = sorted(
            [(float(p), float(q)) for p, q in self.asks.items()],
            key=lambda x: x[0]
        )[:self.max_depth]

        return sorted_bids, sorted_asks

    def build_compressed_snapshot(self) -> bytes:
        """Build 4000-element snapshot array and compress with zlib.

        Returns:
            bytes: zlib-compressed snapshot suitable for BYTEA column.

        The array format is:
            [0-999]: Bid prices (best at index 0)
            [1000-1999]: Bid quantities
            [2000-2999]: Ask prices (best at index 2000)
            [3000-3999]: Ask quantities
        """
        snapshot = np.zeros(4000, dtype=np.float64)
        sorted_bids, sorted_asks = self.get_sorted_levels()

        for i, (price, qty) in enumerate(sorted_bids[:DEPTH]):
            snapshot[i] = price
            snapshot[DEPTH + i] = qty

        for i, (price, qty) in enumerate(sorted_asks[:DEPTH]):
            snapshot[2 * DEPTH + i] = price
            snapshot[3 * DEPTH + i] = qty

        return zlib.compress(snapshot.tobytes(), level=6)


def get_minute_timestamp(timestamp_ms: int) -> int:
    """Get the minute boundary timestamp (truncated to minute).

    Args:
        timestamp_ms: Unix timestamp in milliseconds.

    Returns:
        int: Timestamp truncated to minute boundary in milliseconds.
    """
    dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
    minute_dt = dt.replace(second=0, microsecond=0)
    return int(minute_dt.timestamp() * 1000)


def ms_to_datetime(timestamp_ms: int) -> datetime:
    """Convert millisecond timestamp to datetime.

    Args:
        timestamp_ms: Unix timestamp in milliseconds.

    Returns:
        datetime: UTC datetime object.
    """
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)


def format_delta_jsonb(data: dict) -> dict:
    """Format delta data for JSONB storage.

    Args:
        data: Raw delta data from source file.

    Returns:
        dict: Formatted delta for database storage.
    """
    result = {
        'b': [[float(p), float(q)] for p, q in data.get('b', [])],
        'a': [[float(p), float(q)] for p, q in data.get('a', [])]
    }

    if 'u' in data:
        result['u'] = data['u']
    if 'seq' in data:
        result['seq'] = data['seq']

    return result


def extract_archive(archive_path: Path, temp_dir: Path) -> Optional[Path]:
    """Extract archive file and return path to extracted .data file.

    Args:
        archive_path: Path to the archive file (.zip or .gz).
        temp_dir: Directory for extraction.

    Returns:
        Path to extracted .data file, or None if extraction failed.
    """
    try:
        temp_dir.mkdir(parents=True, exist_ok=True)

        if archive_path.suffix == '.zip':
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
        elif archive_path.suffix == '.gz':
            output_file = temp_dir / archive_path.stem
            with gzip.open(archive_path, 'rb') as f_in:
                with open(output_file, 'wb') as f_out:
                    f_out.write(f_in.read())
        else:
            return None

        data_files = list(temp_dir.glob('*.data'))
        if data_files:
            return data_files[0]
        return None
    except Exception as e:
        logger.error(f"Error extracting {archive_path}: {e}")
        return None


def parse_symbol_from_filename(filename: str) -> str:
    """Extract symbol from archive filename.

    Args:
        filename: Archive filename (e.g., '2025-01-15_BTCUSDT_ob200.data.zip').

    Returns:
        str: Symbol name (e.g., 'BTCUSDT').
    """
    parts = filename.split('_')
    if len(parts) >= 2:
        return parts[1]
    return 'UNKNOWN'


def parse_date_from_filename(filename: str) -> date:
    """Extract date from archive filename.

    Args:
        filename: Archive filename (e.g., '2025-01-15_BTCUSDT_ob200.data.zip').

    Returns:
        date: The date from the filename.
    """
    parts = filename.split('_')
    if parts:
        return datetime.strptime(parts[0], '%Y-%m-%d').date()
    return date.today()


async def update_data_ranges(
    conn: asyncpg.Connection,
    symbol_id: int,
    data_type_id: int,
    start_ts: datetime,
    end_ts: datetime,
    record_count: int,
    source_info: Optional[dict] = None
) -> None:
    """Update data_ranges table for the imported data.

    If no range exists for the symbol/data_type, creates a new record.
    If a range exists, extends the end_datetime and updates record_count.

    Args:
        conn: Database connection.
        symbol_id: ID of the symbol.
        data_type_id: ID of the data type (8=snapshot, 9=delta).
        start_ts: Start timestamp of the imported data.
        end_ts: End timestamp of the imported data.
        record_count: Number of records imported.
        source_info: Optional JSON metadata about the source.
    """
    existing = await conn.fetchrow(
        """
        SELECT id, begin_datetime, end_datetime, record_count
        FROM data_ranges
        WHERE symbol_id = $1
          AND data_type_id = $2
          AND range_type_id = $3
          AND segment_type = 'data'
        ORDER BY sequence_number DESC
        LIMIT 1
        """,
        symbol_id, data_type_id, RANGE_TYPE_RAW_DATA
    )

    if existing:
        new_end = max(existing['end_datetime'], end_ts)
        new_count = existing['record_count'] + record_count

        await conn.execute(
            """
            UPDATE data_ranges
            SET end_datetime = $1,
                record_count = $2,
                updated_at = NOW()
            WHERE id = $3
            """,
            new_end, new_count, existing['id']
        )
        logger.debug(
            f"Updated data_ranges id={existing['id']}: "
            f"end_datetime={new_end}, record_count={new_count}"
        )
    else:
        await conn.execute(
            """
            INSERT INTO data_ranges (
                symbol_id, data_type_id, range_type_id, segment_type,
                sequence_number, begin_datetime, end_datetime,
                record_count, source_info
            ) VALUES ($1, $2, $3, 'data', 1, $4, $5, $6, $7)
            """,
            symbol_id, data_type_id, RANGE_TYPE_RAW_DATA,
            start_ts, end_ts, record_count,
            json.dumps(source_info) if source_info else None
        )
        logger.debug(
            f"Created data_ranges for symbol_id={symbol_id}, "
            f"data_type_id={data_type_id}: {start_ts} to {end_ts}"
        )


class OrderbookDatabaseImporter:
    """Imports historical orderbook data to FinancialDB_V2.

    Processes .data files from .zip archives, importing deltas to
    orderbook_deltas_archive and minute snapshots to orderbook_archive.
    Successfully imported files are moved to ImportedFiles directory.

    Attributes:
        db_manager: Database connection manager.
        partition_manager: Partition management utility.
        symbol_map: Mapping of symbol names to database IDs.
        batch_size_deltas: Number of deltas per batch insert.
        batch_size_snapshots: Number of snapshots per batch insert.
        import_deltas: Whether to import raw deltas.
        import_snapshots: Whether to import minute snapshots.
    """

    def __init__(
        self,
        db_manager: DatabaseConnectionManager,
        batch_size_deltas: int = BATCH_SIZE_DELTAS,
        batch_size_snapshots: int = BATCH_SIZE_SNAPSHOTS,
        import_deltas: bool = True,
        import_snapshots: bool = True
    ):
        """Initialize the importer.

        Args:
            db_manager: Database connection manager.
            batch_size_deltas: Number of deltas per batch insert.
            batch_size_snapshots: Number of snapshots per batch insert.
            import_deltas: Whether to import raw deltas.
            import_snapshots: Whether to import minute snapshots.
        """
        self.db_manager = db_manager
        self.partition_manager = PartitionManager()
        self.symbol_map: Dict[str, int] = {}
        self.batch_size_deltas = batch_size_deltas
        self.batch_size_snapshots = batch_size_snapshots
        self.import_deltas = import_deltas
        self.import_snapshots = import_snapshots

    async def initialize(self) -> None:
        """Initialize the importer by loading symbol mapping."""
        async with self.db_manager.acquire() as conn:
            self.symbol_map = await load_symbol_mapping(conn)
            logger.info(f"Loaded {len(self.symbol_map)} symbol mappings")

    async def process_archive(
        self,
        archive_path: Path,
        temp_base_dir: Path,
        imported_base_dir: Path,
        dry_run: bool = False
    ) -> ImportResult:
        """Process a single archive file.

        Args:
            archive_path: Path to the archive file.
            temp_base_dir: Base directory for temporary extraction.
            imported_base_dir: Base directory for imported files.
            dry_run: If True, parse without inserting or moving.

        Returns:
            ImportResult: Result of the import operation.
        """
        symbol = parse_symbol_from_filename(archive_path.name)
        file_date = parse_date_from_filename(archive_path.name)

        if symbol not in self.symbol_map:
            return ImportResult(
                success=False,
                file_path=archive_path,
                symbol=symbol,
                error_message=f"Unknown symbol: {symbol}"
            )

        symbol_id = self.symbol_map[symbol]
        temp_dir = temp_base_dir / f"temp_{archive_path.stem}"

        try:
            data_file = extract_archive(archive_path, temp_dir)
            if not data_file:
                return ImportResult(
                    success=False,
                    file_path=archive_path,
                    symbol=symbol,
                    error_message="Failed to extract archive"
                )

            deltas_batch = []
            snapshots_batch = []
            orderbook = OrderbookState()
            last_minute = None
            first_ts = None
            last_ts = None
            deltas_imported = 0
            snapshots_imported = 0
            deltas_skipped = 0

            async with self.db_manager.acquire() as conn:
                await self.partition_manager.ensure_partitions_for_date(
                    conn, file_date
                )

                with open(data_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            msg = json.loads(line)
                            timestamp = msg.get('ts', msg.get('timestamp'))
                            msg_type = msg.get('type')
                            data = msg.get('data', {})

                            if first_ts is None:
                                first_ts = timestamp
                            last_ts = timestamp

                            if msg_type == 'snapshot':
                                orderbook.apply_snapshot(data, timestamp)
                            elif msg_type == 'delta':
                                orderbook.apply_delta(data, timestamp)

                                if self.import_deltas and not dry_run:
                                    delta_jsonb = format_delta_jsonb(data)
                                    deltas_batch.append((
                                        symbol_id,
                                        ms_to_datetime(timestamp),
                                        json.dumps(delta_jsonb)
                                    ))

                                    if len(deltas_batch) >= self.batch_size_deltas:
                                        inserted, skipped = await self._batch_insert_deltas(
                                            conn, deltas_batch
                                        )
                                        deltas_imported += inserted
                                        deltas_skipped += skipped
                                        deltas_batch = []

                            current_minute = get_minute_timestamp(timestamp)

                            if self.import_snapshots and last_minute is not None:
                                if current_minute > last_minute:
                                    snapshot_compressed = orderbook.build_compressed_snapshot()
                                    snapshots_batch.append((
                                        symbol_id,
                                        ms_to_datetime(last_minute),
                                        snapshot_compressed
                                    ))

                                    if len(snapshots_batch) >= self.batch_size_snapshots:
                                        if not dry_run:
                                            inserted = await self._batch_insert_snapshots(
                                                conn, snapshots_batch
                                            )
                                            snapshots_imported += inserted
                                        snapshots_batch = []

                            last_minute = current_minute

                        except json.JSONDecodeError:
                            continue
                        except Exception as e:
                            logger.warning(f"Error processing line: {e}")
                            continue

                if deltas_batch and not dry_run:
                    inserted, skipped = await self._batch_insert_deltas(
                        conn, deltas_batch
                    )
                    deltas_imported += inserted
                    deltas_skipped += skipped

                if snapshots_batch and not dry_run:
                    inserted = await self._batch_insert_snapshots(
                        conn, snapshots_batch
                    )
                    snapshots_imported += inserted

                # Update data_ranges for delta and snapshot data
                if not dry_run and first_ts and last_ts:
                    start_dt = ms_to_datetime(first_ts)
                    end_dt = ms_to_datetime(last_ts)

                    if self.import_deltas and deltas_imported > 0:
                        await update_data_ranges(
                            conn, symbol_id, DATA_TYPE_ORDERBOOK_DELTA,
                            start_dt, end_dt, deltas_imported,
                            {'source_file': archive_path.name}
                        )

                    if self.import_snapshots and snapshots_imported > 0:
                        await update_data_ranges(
                            conn, symbol_id, DATA_TYPE_ORDERBOOK_SNAPSHOT,
                            start_dt, end_dt, snapshots_imported,
                            {'source_file': archive_path.name}
                        )

            if not dry_run:
                self._move_to_imported(archive_path, imported_base_dir)

            return ImportResult(
                success=True,
                file_path=archive_path,
                symbol=symbol,
                deltas_imported=deltas_imported,
                snapshots_imported=snapshots_imported,
                deltas_skipped=deltas_skipped,
                data_start_ts=ms_to_datetime(first_ts) if first_ts else None,
                data_end_ts=ms_to_datetime(last_ts) if last_ts else None
            )

        except Exception as e:
            logger.error(f"Error processing {archive_path}: {e}")
            return ImportResult(
                success=False,
                file_path=archive_path,
                symbol=symbol,
                error_message=str(e)
            )

        finally:
            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)

    async def _batch_insert_deltas(
        self,
        conn: asyncpg.Connection,
        deltas: List[Tuple[int, datetime, str]]
    ) -> Tuple[int, int]:
        """Batch insert deltas to orderbook_deltas_archive.

        Args:
            conn: Database connection.
            deltas: List of (symbol_id, timestamp, delta_json) tuples.

        Returns:
            Tuple of (inserted_count, skipped_count).
        """
        query = """
            INSERT INTO orderbook_deltas_archive (symbol_id, delta_timestamp, delta_data)
            VALUES ($1, $2, $3::jsonb)
            ON CONFLICT (delta_timestamp, symbol_id) DO NOTHING
        """

        try:
            result = await conn.executemany(query, deltas)
            return len(deltas), 0
        except asyncpg.UniqueViolationError:
            return 0, len(deltas)

    async def _batch_insert_snapshots(
        self,
        conn: asyncpg.Connection,
        snapshots: List[Tuple[int, datetime, bytes]]
    ) -> int:
        """Batch insert compressed snapshots to orderbook_archive.

        Args:
            conn: Database connection.
            snapshots: List of (symbol_id, minute_ts, compressed_data) tuples.

        Returns:
            int: Number of snapshots inserted.
        """
        query = """
            INSERT INTO orderbook_archive (symbol_id, minute_ts, snapshot_compressed)
            VALUES ($1, $2, $3)
            ON CONFLICT (minute_ts, symbol_id) DO NOTHING
        """

        await conn.executemany(query, snapshots)
        return len(snapshots)

    def _move_to_imported(self, archive_path: Path, imported_base_dir: Path) -> None:
        """Move successfully imported file to ImportedFiles directory.

        Preserves the directory structure (0200/SYMBOL/ or 0500/SYMBOL/).

        Args:
            archive_path: Path to the archive file.
            imported_base_dir: Base directory for imported files.
        """
        relative_parts = []
        current = archive_path.parent
        while current.name and current.name not in ('OrderBook', ''):
            relative_parts.insert(0, current.name)
            current = current.parent

        if relative_parts:
            dest_dir = imported_base_dir / Path(*relative_parts)
        else:
            dest_dir = imported_base_dir

        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / archive_path.name

        shutil.move(str(archive_path), str(dest_path))
        logger.debug(f"Moved {archive_path.name} to {dest_path}")


async def process_directory(
    source_dir: Path,
    imported_dir: Path,
    temp_dir: Path,
    db_manager: DatabaseConnectionManager,
    concurrency: int = DEFAULT_CONCURRENCY,
    import_deltas: bool = True,
    import_snapshots: bool = True,
    dry_run: bool = False
) -> ImportSummary:
    """Process all orderbook files in the source directory.

    Args:
        source_dir: Source directory containing 0200/ and 0500/ subdirectories.
        imported_dir: Directory for successfully imported files.
        temp_dir: Temporary directory for extraction.
        db_manager: Database connection manager.
        concurrency: Maximum concurrent file processing.
        import_deltas: Whether to import raw deltas.
        import_snapshots: Whether to import minute snapshots.
        dry_run: If True, parse without inserting or moving.

    Returns:
        ImportSummary: Summary of the import operation.
    """
    importer = OrderbookDatabaseImporter(
        db_manager,
        import_deltas=import_deltas,
        import_snapshots=import_snapshots
    )
    await importer.initialize()

    files_to_process = []

    for depth_dir in ['0200', '0500']:
        depth_source_dir = source_dir / depth_dir
        if not depth_source_dir.exists():
            continue

        for pair_dir in depth_source_dir.iterdir():
            if not pair_dir.is_dir():
                continue

            archive_files = list(pair_dir.glob('*.zip')) + list(pair_dir.glob('*.gz'))
            files_to_process.extend(archive_files)

    total_files = len(files_to_process)
    logger.info(f"Found {total_files} archive files to process")

    if total_files == 0:
        return ImportSummary()

    summary = ImportSummary(total_files=total_files)
    semaphore = asyncio.Semaphore(concurrency)

    async def process_with_semaphore(archive_path: Path, file_num: int) -> ImportResult:
        async with semaphore:
            return await importer.process_archive(
                archive_path, temp_dir, imported_dir, dry_run
            )

    tasks = [
        process_with_semaphore(f, i + 1)
        for i, f in enumerate(files_to_process)
    ]

    for i, coro in enumerate(asyncio.as_completed(tasks), 1):
        try:
            result = await coro

            if result.success:
                summary.successful += 1
                summary.total_deltas += result.deltas_imported
                summary.total_snapshots += result.snapshots_imported
                status = "IMPORT" if not dry_run else "DRY-RUN"
                print(
                    f"[{i}/{total_files}] {status:8} {result.symbol:10} "
                    f"{result.file_path.name:45} "
                    f"-> {result.deltas_imported} deltas, "
                    f"{result.snapshots_imported} snapshots"
                )
            else:
                summary.failed += 1
                summary.errors.append(f"{result.file_path.name}: {result.error_message}")
                print(
                    f"[{i}/{total_files}] ERROR    {result.symbol:10} "
                    f"{result.file_path.name:45} -> {result.error_message}"
                )

        except Exception as e:
            summary.failed += 1
            summary.errors.append(str(e))
            logger.error(f"Task failed: {e}")

    return summary


async def run_test_mode(
    db_manager: DatabaseConnectionManager,
    source_dir: Path,
    imported_dir: Path,
    temp_dir: Path
) -> bool:
    """Run test mode: import one file and verify database contents.

    This test mode:
    1. Finds a single test file to process
    2. Imports deltas and snapshots
    3. Verifies database contents match expected counts
    4. Confirms file was moved to ImportedFiles
    5. Optionally cleans up test data

    Args:
        db_manager: Database connection manager.
        source_dir: Source directory with test files.
        imported_dir: Directory for imported files.
        temp_dir: Temporary extraction directory.

    Returns:
        bool: True if all tests passed, False otherwise.
    """
    print("\n" + "=" * 80)
    print("TEST MODE - End-to-End Verification")
    print("=" * 80)

    # Find a test file
    test_file = None
    for depth_dir in ['0200', '0500']:
        depth_path = source_dir / depth_dir
        if not depth_path.exists():
            continue
        for pair_dir in depth_path.iterdir():
            if not pair_dir.is_dir():
                continue
            archives = list(pair_dir.glob('*.zip')) + list(pair_dir.glob('*.gz'))
            if archives:
                test_file = archives[0]
                break
        if test_file:
            break

    if not test_file:
        print("ERROR: No test file found in source directory")
        return False

    symbol = parse_symbol_from_filename(test_file.name)
    file_date = parse_date_from_filename(test_file.name)

    print(f"\nTest file: {test_file.name}")
    print(f"Symbol: {symbol}")
    print(f"Date: {file_date}")

    # Get symbol_id
    async with db_manager.acquire() as conn:
        symbol_map = await load_symbol_mapping(conn)

    if symbol not in symbol_map:
        print(f"ERROR: Symbol {symbol} not found in database")
        return False

    symbol_id = symbol_map[symbol]
    print(f"Symbol ID: {symbol_id}")

    # Get baseline counts before import
    async with db_manager.acquire() as conn:
        delta_count_before = await conn.fetchval(
            """
            SELECT COUNT(*) FROM orderbook_deltas_archive
            WHERE symbol_id = $1
              AND delta_timestamp >= $2
              AND delta_timestamp < $3
            """,
            symbol_id,
            datetime.combine(file_date, datetime.min.time()).replace(tzinfo=timezone.utc),
            datetime.combine(file_date, datetime.min.time()).replace(tzinfo=timezone.utc) + timedelta(days=1)
        )

        snapshot_count_before = await conn.fetchval(
            """
            SELECT COUNT(*) FROM orderbook_archive
            WHERE symbol_id = $1
              AND minute_ts >= $2
              AND minute_ts < $3
            """,
            symbol_id,
            datetime.combine(file_date, datetime.min.time()).replace(tzinfo=timezone.utc),
            datetime.combine(file_date, datetime.min.time()).replace(tzinfo=timezone.utc) + timedelta(days=1)
        )

    print(f"\nBaseline counts for {file_date}:")
    print(f"  Deltas: {delta_count_before}")
    print(f"  Snapshots: {snapshot_count_before}")

    # Run import
    print("\n--- IMPORTING ---")
    importer = OrderbookDatabaseImporter(db_manager)
    await importer.initialize()

    result = await importer.process_archive(
        test_file, temp_dir, imported_dir, dry_run=False
    )

    if not result.success:
        print(f"ERROR: Import failed - {result.error_message}")
        return False

    print(f"\nImport result:")
    print(f"  Deltas imported: {result.deltas_imported:,}")
    print(f"  Snapshots imported: {result.snapshots_imported:,}")
    print(f"  Data range: {result.data_start_ts} to {result.data_end_ts}")

    # Verify database contents
    print("\n--- VERIFYING DATABASE ---")
    all_passed = True

    async with db_manager.acquire() as conn:
        # Check delta count
        delta_count_after = await conn.fetchval(
            """
            SELECT COUNT(*) FROM orderbook_deltas_archive
            WHERE symbol_id = $1
              AND delta_timestamp >= $2
              AND delta_timestamp < $3
            """,
            symbol_id,
            datetime.combine(file_date, datetime.min.time()).replace(tzinfo=timezone.utc),
            datetime.combine(file_date, datetime.min.time()).replace(tzinfo=timezone.utc) + timedelta(days=1)
        )

        delta_inserted = delta_count_after - delta_count_before
        if delta_inserted == result.deltas_imported:
            print(f"  [PASS] Deltas: {delta_inserted:,} records inserted")
        else:
            print(f"  [FAIL] Deltas: Expected {result.deltas_imported:,}, found {delta_inserted:,}")
            all_passed = False

        # Check snapshot count
        snapshot_count_after = await conn.fetchval(
            """
            SELECT COUNT(*) FROM orderbook_archive
            WHERE symbol_id = $1
              AND minute_ts >= $2
              AND minute_ts < $3
            """,
            symbol_id,
            datetime.combine(file_date, datetime.min.time()).replace(tzinfo=timezone.utc),
            datetime.combine(file_date, datetime.min.time()).replace(tzinfo=timezone.utc) + timedelta(days=1)
        )

        snapshot_inserted = snapshot_count_after - snapshot_count_before
        if snapshot_inserted == result.snapshots_imported:
            print(f"  [PASS] Snapshots: {snapshot_inserted:,} records inserted")
        else:
            print(f"  [FAIL] Snapshots: Expected {result.snapshots_imported:,}, found {snapshot_inserted:,}")
            all_passed = False

        # Check sample delta JSONB format
        sample_delta = await conn.fetchrow(
            """
            SELECT delta_data FROM orderbook_deltas_archive
            WHERE symbol_id = $1
              AND delta_timestamp >= $2
            ORDER BY delta_timestamp
            LIMIT 1
            """,
            symbol_id,
            datetime.combine(file_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        )

        if sample_delta:
            delta_data = sample_delta['delta_data']
            if 'b' in delta_data and 'a' in delta_data:
                print(f"  [PASS] Delta JSONB format valid (has 'b' and 'a' keys)")
            else:
                print(f"  [FAIL] Delta JSONB format invalid: {delta_data}")
                all_passed = False
        else:
            print(f"  [WARN] No sample delta found to verify format")

        # Check sample snapshot compression
        sample_snapshot = await conn.fetchrow(
            """
            SELECT minute_ts, length(snapshot_compressed) as size
            FROM orderbook_archive
            WHERE symbol_id = $1
              AND minute_ts >= $2
            ORDER BY minute_ts
            LIMIT 1
            """,
            symbol_id,
            datetime.combine(file_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        )

        if sample_snapshot:
            compressed_size = sample_snapshot['size']
            uncompressed_size = 4000 * 8  # 4000 float64 = 32000 bytes
            ratio = uncompressed_size / compressed_size if compressed_size > 0 else 0
            print(f"  [PASS] Snapshot compression: {compressed_size:,} bytes (ratio: {ratio:.1f}x)")
        else:
            print(f"  [WARN] No sample snapshot found to verify compression")

        # Check data_ranges was updated
        delta_range = await conn.fetchrow(
            """
            SELECT begin_datetime, end_datetime, record_count
            FROM data_ranges
            WHERE symbol_id = $1 AND data_type_id = $2
            ORDER BY sequence_number DESC LIMIT 1
            """,
            symbol_id, DATA_TYPE_ORDERBOOK_DELTA
        )

        if delta_range:
            print(f"  [PASS] Delta data_ranges: {delta_range['begin_datetime']} to {delta_range['end_datetime']} ({delta_range['record_count']:,} records)")
        else:
            print(f"  [FAIL] Delta data_ranges not created")
            all_passed = False

        snapshot_range = await conn.fetchrow(
            """
            SELECT begin_datetime, end_datetime, record_count
            FROM data_ranges
            WHERE symbol_id = $1 AND data_type_id = $2
            ORDER BY sequence_number DESC LIMIT 1
            """,
            symbol_id, DATA_TYPE_ORDERBOOK_SNAPSHOT
        )

        if snapshot_range:
            print(f"  [PASS] Snapshot data_ranges: {snapshot_range['begin_datetime']} to {snapshot_range['end_datetime']} ({snapshot_range['record_count']:,} records)")
        else:
            print(f"  [FAIL] Snapshot data_ranges not created")
            all_passed = False

    # Check file was moved
    print("\n--- VERIFYING FILE MOVE ---")
    original_exists = test_file.exists()

    # Calculate expected destination
    relative_parts = []
    current = test_file.parent
    while current.name and current.name not in ('OrderBook', ''):
        relative_parts.insert(0, current.name)
        current = current.parent

    if relative_parts:
        expected_dest = imported_dir / Path(*relative_parts) / test_file.name
    else:
        expected_dest = imported_dir / test_file.name

    moved_exists = expected_dest.exists()

    if not original_exists and moved_exists:
        print(f"  [PASS] File moved to: {expected_dest}")
    elif original_exists and not moved_exists:
        print(f"  [FAIL] File not moved (still at original location)")
        all_passed = False
    elif original_exists and moved_exists:
        print(f"  [WARN] File exists in both locations")
    else:
        print(f"  [FAIL] File missing from both locations")
        all_passed = False

    # Summary
    print("\n" + "=" * 80)
    if all_passed:
        print("TEST RESULT: ALL TESTS PASSED")
    else:
        print("TEST RESULT: SOME TESTS FAILED")
    print("=" * 80)

    return all_passed


def main():
    """Main entry point for the orderbook database importer."""
    parser = argparse.ArgumentParser(
        description='Import Bybit orderbook data to FinancialDB_V2'
    )
    parser.add_argument(
        '--source-dir',
        type=Path,
        default=SOURCE_DIR,
        help='Source directory containing orderbook archives'
    )
    parser.add_argument(
        '--imported-dir',
        type=Path,
        default=IMPORTED_DIR,
        help='Directory for successfully imported files'
    )
    parser.add_argument(
        '--temp-dir',
        type=Path,
        default=TEMP_DIR,
        help='Temporary directory for extraction'
    )
    parser.add_argument(
        '--concurrency',
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f'Maximum concurrent file processing (default: {DEFAULT_CONCURRENCY})'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=BATCH_SIZE_DELTAS,
        help=f'Batch size for delta inserts (default: {BATCH_SIZE_DELTAS})'
    )
    parser.add_argument(
        '--import-deltas',
        action='store_true',
        default=True,
        help='Import raw deltas to orderbook_deltas_archive'
    )
    parser.add_argument(
        '--no-deltas',
        action='store_false',
        dest='import_deltas',
        help='Skip delta import (snapshots only)'
    )
    parser.add_argument(
        '--import-snapshots',
        action='store_true',
        default=True,
        help='Import minute snapshots to orderbook_archive'
    )
    parser.add_argument(
        '--no-snapshots',
        action='store_false',
        dest='import_snapshots',
        help='Skip snapshot import (deltas only)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Parse and validate without inserting or moving files'
    )
    parser.add_argument(
        '--database-url',
        type=str,
        default=None,
        help='Database connection URL (default: uses DATABASE_URL env or built-in default)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run test mode: import one file and verify database contents'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Test mode
    if args.test:
        async def run_test():
            db_manager = DatabaseConnectionManager(dsn=args.database_url)
            async with db_manager:
                success = await run_test_mode(
                    db_manager,
                    args.source_dir,
                    args.imported_dir,
                    args.temp_dir
                )
            return success

        success = asyncio.run(run_test())
        exit(0 if success else 1)

    # Normal mode
    print("=" * 80)
    print("Bybit Orderbook Database Importer")
    print("=" * 80)
    print(f"Source: {args.source_dir}")
    print(f"Imported files: {args.imported_dir}")
    print(f"Temp: {args.temp_dir}")
    print(f"Concurrency: {args.concurrency}")
    print(f"Import deltas: {'Yes' if args.import_deltas else 'No'}")
    print(f"Import snapshots: {'Yes' if args.import_snapshots else 'No'}")
    print(f"Dry run: {'Yes' if args.dry_run else 'No'}")
    print("=" * 80 + "\n")

    async def run():
        db_manager = DatabaseConnectionManager(dsn=args.database_url)

        async with db_manager:
            summary = await process_directory(
                args.source_dir,
                args.imported_dir,
                args.temp_dir,
                db_manager,
                args.concurrency,
                args.import_deltas,
                args.import_snapshots,
                args.dry_run
            )

        print("\n" + "=" * 80)
        print("Import Complete!")
        print(f"Total files: {summary.total_files}")
        print(f"Successful: {summary.successful}")
        print(f"Failed: {summary.failed}")
        print(f"Total deltas imported: {summary.total_deltas:,}")
        print(f"Total snapshots imported: {summary.total_snapshots:,}")

        if summary.errors:
            print(f"\nErrors ({len(summary.errors)}):")
            for error in summary.errors[:10]:
                print(f"  - {error}")
            if len(summary.errors) > 10:
                print(f"  ... and {len(summary.errors) - 10} more")

        print("=" * 80)

    asyncio.run(run())


if __name__ == '__main__':
    main()

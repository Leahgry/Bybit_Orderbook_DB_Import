# Bybit Orderbook Database Importer

Import Bybit historical orderbook data from `.data` files in `.zip` archives into FinancialDB_V2 PostgreSQL database.

## Overview

This tool processes orderbook files containing snapshot and delta updates:
- **Deltas** → `orderbook_deltas_archive` table (JSONB format)
- **Minute Snapshots** → `orderbook_archive` table (zlib compressed)

Successfully imported files are moved to an `ImportedFiles` directory to track progress across runs.

## Requirements

- Python 3.9+
- PostgreSQL 14+ with FinancialDB_V2 database
- Required partitions and tables (see FinancialDB_V2 migrations)

## Installation

```bash
cd Bybit_Orderbook_DB_Import
pip install -r requirements.txt
```

## Usage

### Basic Import

```bash
python3 import_orderbook_to_db.py
```

### With Custom Paths

```bash
python3 import_orderbook_to_db.py \
  --source-dir /path/to/OrderBook \
  --imported-dir /path/to/OrderBook/ImportedFiles
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--source-dir` | `/Volumes/Backup01/FinancialData/Bybit/OrderBook` | Source directory with 0200/ and 0500/ subdirs |
| `--imported-dir` | `<source>/ImportedFiles` | Directory for successfully imported files |
| `--temp-dir` | `/tmp/orderbook_db_import` | Temporary extraction directory |
| `--concurrency` | 4 | Number of concurrent file imports |
| `--batch-size` | 2000 | Records per batch insert |
| `--no-deltas` | | Skip delta import (snapshots only) |
| `--no-snapshots` | | Skip snapshot import (deltas only) |
| `--dry-run` | | Parse without inserting or moving files |
| `--database-url` | | Override database connection URL |
| `-v, --verbose` | | Enable debug logging |

## Source Directory Structure

```
/OrderBook/
├── 0200/                        # 200-level orderbook data
│   ├── BTCUSDT/
│   │   ├── 2025-01-15_BTCUSDT_ob200.data.zip
│   │   └── ...
│   └── ETHUSDT/
│       └── ...
└── 0500/                        # 500-level orderbook data
    ├── BTCUSDT/
    │   └── ...
    └── ...
```

## Target Database Tables

### orderbook_deltas_archive

Stores raw delta updates in JSONB format:

```sql
CREATE TABLE orderbook_deltas_archive (
    id BIGSERIAL,
    symbol_id INTEGER NOT NULL,
    delta_timestamp TIMESTAMPTZ NOT NULL,
    delta_data JSONB NOT NULL,
    archived_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (delta_timestamp, symbol_id)
) PARTITION BY RANGE (delta_timestamp);
```

**JSONB Format:**
```json
{
    "b": [[42150.5, 1.25], [42150.0, 0.0]],
    "a": [[42151.0, 2.5]],
    "u": 1234567890
}
```

### orderbook_archive

Stores 1-minute snapshots as compressed BYTEA:

```sql
CREATE TABLE orderbook_archive (
    id BIGSERIAL,
    symbol_id INTEGER NOT NULL,
    minute_ts TIMESTAMPTZ NOT NULL,
    snapshot_compressed BYTEA,
    archived_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (minute_ts, symbol_id)
) PARTITION BY RANGE (minute_ts);
```

**Snapshot Array Format (4000 elements):**
| Index Range | Content |
|-------------|---------|
| 0-999 | Bid prices (best at index 0) |
| 1000-1999 | Bid quantities |
| 2000-2999 | Ask prices (best at index 2000) |
| 3000-3999 | Ask quantities |

### data_ranges

Tracks available data ranges per symbol:

```sql
CREATE TABLE data_ranges (
    symbol_id INTEGER NOT NULL,
    data_type_id INTEGER NOT NULL,
    range_type_id INTEGER NOT NULL,
    begin_datetime TIMESTAMPTZ NOT NULL,
    end_datetime TIMESTAMPTZ NOT NULL,
    record_count BIGINT,
    ...
);
```

## Symbol Mapping

The importer loads symbol mappings from the `symbols` table:

| symbol_id | symbol |
|-----------|--------|
| 1 | BTCUSDT |
| 2 | ETHUSDT |
| 3 | SOLUSDT |
| 4 | XRPUSDT |
| 5 | DOGEUSDT |
| 6 | ADAUSDT |
| 7 | AVAXUSDT |
| 8 | LINKUSDT |
| 9 | BNBUSDT |
| 10 | LTCUSDT |
| 11 | TRXUSDT |
| 12 | MNTUSDT |

## Import Tracking

Successfully imported files are moved to maintain progress:

```
/OrderBook/
├── 0200/BTCUSDT/                      # Pending files
│   └── 2025-01-20_BTCUSDT_ob200.data.zip
└── ImportedFiles/
    └── 0200/BTCUSDT/                  # Imported files
        ├── 2025-01-15_BTCUSDT_ob200.data.zip
        └── 2025-01-16_BTCUSDT_ob200.data.zip
```

This allows safe resumption - just run the importer again and it will only process new files.

## Performance

| Metric | Target |
|--------|--------|
| Delta inserts/second | 5,000-20,000 |
| Snapshot inserts/second | 100-500 |
| Batch size (deltas) | 2,000 records |
| Batch size (snapshots) | 100 records |
| Concurrency | 4 files (configurable) |

## Verification Queries

After import, verify data integrity:

```sql
-- Count records by symbol and month
SELECT
    s.symbol,
    date_trunc('month', delta_timestamp) as month,
    COUNT(*) as delta_count
FROM orderbook_deltas_archive oda
JOIN symbols s ON s.id = oda.symbol_id
GROUP BY s.symbol, month
ORDER BY s.symbol, month;

-- Check snapshot compression ratio
SELECT
    symbol_id,
    minute_ts,
    length(snapshot_compressed) as compressed_size,
    32000 as uncompressed_size,
    round(32000.0 / length(snapshot_compressed), 2) as ratio
FROM orderbook_archive
LIMIT 10;

-- Verify data_ranges
SELECT
    s.symbol,
    dt.name as data_type,
    dr.begin_datetime,
    dr.end_datetime,
    dr.record_count
FROM data_ranges dr
JOIN symbols s ON s.id = dr.symbol_id
JOIN data_types dt ON dt.id = dr.data_type_id
WHERE dt.name LIKE 'source_orderbook%'
ORDER BY s.symbol, dt.name;
```

## Error Handling

| Error | Action |
|-------|--------|
| Unknown symbol | Skip file, log error |
| Missing partition | Auto-create partition |
| Duplicate records | Skip (ON CONFLICT DO NOTHING) |
| Corrupt archive | Skip file, log error |
| Connection lost | Retry with backoff |

## Database Connection

Default connection (can be overridden with `--database-url`):

```
postgresql://andy_henderson@localhost:5433/FinancialDB_V2
```

Or set the `DATABASE_URL` environment variable.

## Related Documentation

- [Historical Orderbook Import Requirements](docs/historical/orderbook_historical_import_requirements.md)
- [FinancialDB_V2 Schema](docs/schema/)

# Bybit Orderbook to 1-Minute Snapshot Converter

This script converts Bybit orderbook data (snapshots + deltas) to 1-minute interval snapshots in CSV format.

## ⚡ Two Versions Available

This repository includes **two versions** of the converter:

| Version | File | Best For | Speed |
|---------|------|----------|-------|
| **Single-threaded** | `convert_orderbook_to_1min.py` | Small batches, debugging | 1-2 files/min (1 CPU core) |
| **Parallel** | `convert_orderbook_to_1min_parallel.py` | Large batches (1000+ files) | 10-30 files/min (all cores) |

📖 **See `README_PARALLEL.md` for parallel version documentation**

This README documents the **single-threaded version**. For processing thousands of files, the parallel version is **10-15x faster**.

## Overview

The script processes orderbook files containing:
- **Snapshot messages**: Complete orderbook state at a point in time
- **Delta messages**: Incremental updates to the orderbook

It reconstructs the orderbook state by applying deltas to snapshots and extracts the orderbook state at each 1-minute boundary.

**Key Features:**
- **Smart depth generation**: 200-level sources → 200-level output only; 500-level sources → both 200 and 500-level outputs
- Optional formatted timestamp column (ISO 8601 format)
- Condensed output with progress tracking ([X/TOTAL] format)
- Outputs uncompressed CSV files (due to significant size reduction)
- Automatically skips already-processed files (checks for existing outputs)
- Cleans up temporary extraction files automatically
- Preserves original compressed archive files

## Output Format

The output is a CSV file with the following structure:

### Header Row

**With formatted timestamp (default OFF):**
```
timestamp,bid_price_1,bid_qty_1,bid_price_2,bid_qty_2,...,bid_price_N,bid_qty_N,ask_price_1,ask_qty_1,...,ask_price_N,ask_qty_N
```

**With formatted timestamp enabled:**
```
timestamp_formatted,timestamp,bid_price_1,bid_qty_1,...
```

### Data Rows
- **timestamp_formatted** (optional): Human-readable timestamp in `yyyy-mm-ddThh:mm:ss.ssss` format
- **timestamp**: Unix timestamp in milliseconds (at minute boundary: HH:MM:00.000)
- **bid_price_X**: Price of bid level X (sorted highest to lowest)
- **bid_qty_X**: Quantity at bid level X
- **ask_price_X**: Price of ask level X (sorted lowest to highest)
- **ask_qty_X**: Quantity at ask level X

**Column counts:**
- 200-level depth: 801 columns (1 timestamp + 400 bid + 400 ask) or 802 with formatted timestamp
- 500-level depth: 2001 columns (1 timestamp + 1000 bid + 1000 ask) or 2002 with formatted timestamp

## Configuration

Edit the script to modify these settings:

```python
SOURCE_DIR = Path("/volumes/Data01/FinancialData/Bybit/OrderBook")
OUTPUT_DIR = Path("/volumes/Data01/FinancialData/Bybit/OrderBook_1m")
TEMP_DIR = Path("/tmp/orderbook_conversion")
INCLUDE_FORMATTED_TIMESTAMP = False  # Set to True to include human-readable timestamp
```

## Directory Structure

### Input Structure
```
/volumes/Data01/FinancialData/Bybit/OrderBook/
├── 0200/                    # 200-level orderbook data
│   ├── BTCUSDT/
│   │   ├── 2025-01-01_BTCUSDT_ob200.data.zip
│   │   └── ...
│   ├── ETHUSDT/
│   └── ...
└── 0500/                    # 500-level orderbook data
    ├── BTCUSDT/
    └── ...
```

### Output Structure
```
/volumes/Data01/FinancialData/Bybit/OrderBook_1m/
├── 0200/                       # 200-level 1-minute snapshots
│   ├── BTCUSDT/
│   │   ├── 2025-01-01_BTCUSDT_ob200_1m.csv  (from 0200 source)
│   │   ├── 2025-01-02_BTCUSDT_ob500_1m.csv  (from 0500 source, truncated to 200)
│   │   └── ...
│   ├── ETHUSDT/
│   └── ...
└── 0500/                       # 500-level 1-minute snapshots
    ├── BTCUSDT/
    │   ├── 2025-01-02_BTCUSDT_ob500_1m.csv  (from 0500 source only!)
    │   └── ...
    ├── ETHUSDT/
    └── ...
```

**Output logic:**
- **200-level sources** (`0200/` folder): Generate **only** 200-level output → saved to `0200/{PAIR}/`
- **500-level sources** (`0500/` folder): Generate **both** 200-level and 500-level outputs → saved to `0200/{PAIR}/` and `0500/{PAIR}/`

This makes sense because you can't create valid 500-level data from a 200-level source, but you can create 200-level data from a 500-level source (it's the top 200 levels).

## Usage

### Test Mode (Process sample files in project root)
```bash
python3 convert_orderbook_to_1min.py --test
```

This will process the test files in the project directory and output to `test_output/`.

### Production Mode (Process all files)
```bash
python3 convert_orderbook_to_1min.py
```

### Command-Line Options
```bash
python3 convert_orderbook_to_1min.py [OPTIONS]

Options:
  --source-dir PATH              Source directory (default: /volumes/Data01/FinancialData/Bybit/OrderBook)
  --output-dir PATH              Output directory (default: /volumes/Data01/FinancialData/Bybit/OrderBook_1m)
  --temp-dir PATH                Temporary directory for extraction (default: /tmp/orderbook_conversion)
  --include-formatted-timestamp  Include human-readable timestamp column (enables formatted timestamp)
  --no-formatted-timestamp       Exclude human-readable timestamp column (default)
  --test                         Test mode: process test files in project root
  -h, --help                     Show help message
```

### Example: Custom Directories
```bash
python3 convert_orderbook_to_1min.py \
  --source-dir /path/to/source \
  --output-dir /path/to/output \
  --temp-dir /path/to/temp
```

## How It Works

1. **Count Files**: Scans all source directories and counts total files to process
2. **Check for Existing Outputs**: Before processing, checks if required outputs already exist (200-only for 200-sources, 200+500 for 500-sources)
3. **Extract Archive**: Extracts compressed source file to temporary directory
4. **Read Input File**: Loads snapshot and delta messages from extracted data file
5. **Build Orderbook State**: 
   - Starts with initial snapshot
   - Applies each delta update sequentially
   - Maintains sorted bid/ask price levels
6. **Extract 1-Minute Snapshots**:
   - Monitors timestamp of each message
   - When crossing a minute boundary (e.g., 10:15:00.000 → 10:16:00.000)
   - Captures the orderbook state at the previous minute mark
7. **Generate Appropriate Depth Levels**:
   - **From 200-level sources**: Creates only 200-level CSV → saved to `0200/{PAIR}/`
   - **From 500-level sources**: Creates both 200-level and 500-level CSVs → saved to `0200/{PAIR}/` and `0500/{PAIR}/`
8. **Write CSV Outputs**:
   - Generates header with column names (optionally includes formatted timestamp)
   - Writes one row per minute with all price levels
   - Empty cells for missing levels
   - Files remain uncompressed (significant size reduction already achieved)
9. **Progress Display**: Shows condensed one-line status per file with [X/TOTAL] counter
10. **Cleanup**: Removes temporary extracted files, preserves original compressed archives

## Data Processing Notes

### Minute Boundaries
The script captures orderbook state at **exact minute boundaries** (seconds = 0, milliseconds = 0).

For example, if you have updates at:
- 10:15:37.123
- 10:15:45.456
- 10:16:02.789

The snapshot at 10:16:00.000 will include all updates through 10:15:45.456.

### Smart Output Generation Based on Source Depth

The script generates outputs based on the source file's depth capability:

**From 200-level sources** (`0200/` folder):
- Generates **only** 200-level output (saved to `0200/{PAIR}/`)
- 801 columns: 1 timestamp + 400 bid + 400 ask (or 802 with formatted timestamp)
- You cannot create valid 500-level data from 200-level source

**From 500-level sources** (`0500/` folder):
- Generates **both** 200-level AND 500-level outputs
- 200-level output (saved to `0200/{PAIR}/`): Top 200 levels from the 500-level data
- 500-level output (saved to `0500/{PAIR}/`): Full 500 levels
- 200-level: 801 columns (or 802 with formatted timestamp)
- 500-level: 2001 columns (or 2002 with formatted timestamp)

This approach ensures you only create valid, meaningful output files.

### Delta Application
Price levels are updated based on delta messages:
- If quantity = "0" or 0.0, the price level is **removed**
- Otherwise, the price level is **updated** or **added**

### Skip Logic
The script intelligently skips already-processed files:

- Before extracting an archive, checks if required outputs exist based on source type
- **For 200-level sources**: Checks if 200-level output exists
- **For 500-level sources**: Checks if both 200 and 500 level outputs exist
- If all required outputs exist: skips the file entirely (saves time)
- If some outputs are missing: extracts and generates only the missing depth levels
- Original compressed archives are **never** modified or deleted

This makes it safe to re-run the script - it will only process new or incomplete files.

### Progress Tracking
The script provides condensed, informative output:

```
Scanning source directories...
Found 3247 archive files to process

[1/3247]    PROCESS  BTCUSDT    2025-08-21_BTCUSDT_ob200.data.zip   [200] → 1441 snapshots ✓
[2/3247]    SKIP     ETHUSDT    2025-08-22_ETHUSDT_ob200.data.zip   [200] (exists)
[3/3247]    PROCESS  XRPUSDT    2025-08-23_XRPUSDT_ob500.data.zip   [200,500] → 1441 snapshots ✓
```

**Features:**
- **[X/TOTAL]**: Progress counter so you always know where you are
- **STATUS**: PROCESS (generating) or SKIP (already exists)
- **PAIR**: Trading pair name
- **FILENAME**: Source archive filename
- **[DEPTHS]**: Which depth levels being generated ([200] or [200,500])
- **RESULT**: Number of snapshots generated with ✓ for success

This condensed format reduces output by ~90% compared to verbose logging, making it easy to track progress through thousands of files.

## Example Output

```csv
timestamp,bid_price_1,bid_qty_1,bid_price_2,bid_qty_2,ask_price_1,ask_qty_1,ask_price_2,ask_qty_2
1761350400000,1.64740,583.1,1.64720,232.5,1.64750,600.2,1.64760,450.3
1761350460000,1.65090,2029.1,1.65080,301.3,1.65100,750.5,1.65110,325.8
```

## Performance

Processing speed depends on:
- File size (number of messages)
- Update frequency (number of deltas per minute)
- Orderbook depth (200-level processes faster than 500-level)
- Source type (200-sources generate 1 output, 500-sources generate 2 outputs)

**Typical processing times:**
- 200-level source: ~1-2 seconds per file (generates 1 output)
- 500-level source: ~2-4 seconds per file (generates 2 outputs)

**Size Reduction:**
- Original compressed: ~35-65 MB per day
- 200-level output: ~7-8 MB per day (uncompressed CSV)
- 500-level output: ~10-16 MB per day (uncompressed CSV)
- Total reduction: ~3-5x smaller than original

**For 3000+ files:**
- Progress tracking helps estimate completion time
- Condensed output keeps terminal manageable
- Can safely interrupt and resume (skip logic prevents reprocessing)

## Requirements

- Python 3.7+
- Standard library only (no external dependencies)

See `requirements.txt` for details.

## Testing

Test files included in project:
- `2025-10-25_MNTUSDT_ob200.data.zip` - 200-level sample
- `2025-08-19_MNTUSDT_ob500.data.zip` - 500-level sample

Run test mode to verify:
```bash
python3 convert_orderbook_to_1min.py --test
```

Expected output:
- ~1441 snapshots per file (24 hours × 60 minutes + 1)
- **3 CSV files total**:
  - `test_output/0200/MNTUSDT/2025-10-25_MNTUSDT_ob200_ob200_1min.csv` (from 200-source)
  - `test_output/0200/MNTUSDT/2025-08-19_MNTUSDT_ob500_ob200_1min.csv` (from 500-source)
  - `test_output/0500/MNTUSDT/2025-08-19_MNTUSDT_ob500_ob500_1min.csv` (from 500-source)
- Note: The 200-level source only creates 1 file, the 500-level source creates 2 files
- Files are uncompressed CSV
- Temporary extraction files automatically cleaned up

**Test mode uses verbose output for debugging. Production mode uses condensed one-line-per-file output.**


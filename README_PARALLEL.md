# Bybit Orderbook to 1-Minute Snapshot Converter - PARALLEL VERSION

This is a **multi-threaded parallel version** of the orderbook converter that uses Python's `multiprocessing` module to process multiple files simultaneously across all available CPU cores.

## Why Use the Parallel Version?

**Performance Comparison:**
- **Single-threaded version**: ~1-2 files per minute (uses 1 CPU core at 100%)
- **Parallel version**: ~10-30 files per minute (uses all CPU cores, scales with core count)

**When to Use:**
- ✅ Processing large batches (hundreds or thousands of files)
- ✅ Multi-core systems (benefits scale with more cores)
- ✅ When speed is critical

**When to Use Single-Threaded:**
- Processing just a few files
- Debugging or troubleshooting specific files
- Systems with limited memory (parallel uses more RAM)

## Key Differences from Single-Threaded Version

| Feature | Single-Threaded | Parallel |
|---------|----------------|----------|
| **Speed** | 1-2 files/min | 10-30 files/min (scales with cores) |
| **CPU Usage** | 1 core at 100% | All cores at ~80-100% |
| **Memory Usage** | Low | Higher (multiple processes) |
| **Progress Display** | Sequential | Unordered (files complete at different times) |
| **Temp Directory** | Shared | Per-process (isolated) |

## Installation & Requirements

Same as single-threaded version:
- Python 3.7+
- Standard library only (no external dependencies)

See `requirements.txt` for details.

## Usage

### Basic Usage (All CPU Cores)

```bash
python3 convert_orderbook_to_1min_parallel.py
```

This will automatically use all available CPU cores (e.g., 14 cores = 14 parallel workers).

### Custom Number of Workers

If you want to leave some CPU capacity for other tasks:

```bash
# Use 8 workers instead of all cores
python3 convert_orderbook_to_1min_parallel.py --workers 8
```

### With Formatted Timestamps

```bash
python3 convert_orderbook_to_1min_parallel.py --include-formatted-timestamp
```

### Custom Directories

```bash
python3 convert_orderbook_to_1min_parallel.py \
  --source-dir /path/to/source \
  --output-dir /path/to/output \
  --temp-dir /tmp/conversion \
  --workers 12
```

### Test Mode

```bash
python3 convert_orderbook_to_1min_parallel.py --test
```

## Command-Line Options

```
--source-dir SOURCE_DIR    Source directory (default: /volumes/Data01/FinancialData/Bybit/OrderBook)
--output-dir OUTPUT_DIR    Output directory (default: /volumes/Data01/FinancialData/Bybit/OrderBook_1m)
--temp-dir TEMP_DIR        Temporary directory (default: /tmp/orderbook_conversion_parallel)
--workers N                Number of parallel workers (default: CPU core count)
--include-formatted-timestamp   Include ISO 8601 timestamp column
--no-formatted-timestamp        Exclude formatted timestamp (default)
--test                     Run test mode with sample files
```

## How It Works

### Parallel Processing Architecture

1. **Scanning Phase**: Single-threaded scan to identify all files needing processing
2. **Task Distribution**: Creates a task queue with all files to process
3. **Parallel Execution**: 
   - Spawns N worker processes (default = CPU core count)
   - Each worker:
     - Gets a file from the queue
     - Extracts to isolated temp directory (avoids conflicts)
     - Processes the orderbook data
     - Writes output CSV(s)
     - Cleans up its temp directory
     - Reports completion
     - Gets next file from queue
4. **Progress Tracking**: Results displayed as tasks complete (may be out of order)
5. **Summary**: Final statistics and timing

### Process Isolation

Each worker process has:
- Its own temporary extraction directory (`temp_{PID}_{filename}`)
- Independent memory space
- No shared state (avoids race conditions)

This design ensures:
- ✅ No file conflicts
- ✅ Thread-safe operation
- ✅ Fault isolation (one failure doesn't crash others)

## Performance Expectations

### Example System (14-core CPU)

**3000 files to process:**
- Single-threaded: ~25-50 hours
- Parallel (14 workers): ~2-4 hours

**Per-file processing:**
- Typical file: 1-3 seconds per worker
- Large files: 5-10 seconds per worker
- Throughput: ~14 files every 1-3 seconds (with 14 workers)

### Factors Affecting Speed

**Faster:**
- More CPU cores
- Faster SSD storage
- Smaller source files
- Fewer output depths needed

**Slower:**
- Limited CPU cores
- Slow HDD storage
- Large source files (many snapshots)
- Both 200 and 500 depth outputs

## Output Format

Identical to single-threaded version. See main `README.md` for complete details.

## Progress Display

Output shows files completing in real-time (may be out of order):

```
Processing with 14 parallel workers
Scanning source directories...
Found 3247 archive files to process

[1/3247] DONE     BTCUSDT    2025-01-15_BTCUSDT_ob500.data.zip    [200,500] → 1440 snapshots ✓
[3/3247] DONE     ETHUSDT    2025-01-15_ETHUSDT_ob500.data.zip    [200,500] → 1440 snapshots ✓
[2/3247] DONE     SOLUSDT    2025-01-15_SOLUSDT_ob200.data.zip    [200] → 1440 snapshots ✓
[5/3247] SKIPPED  ADAUSDT    2025-01-14_ADAUSDT_ob200.data.zip    [200]
[4/3247] DONE     XRPUSDT    2025-01-15_XRPUSDT_ob500.data.zip    [200,500] → 1440 snapshots ✓
...

================================================================================
Processing complete!
Total files: 3247
Successfully processed: 2891
Skipped (already exist): 312
Errors: 44
Time elapsed: 8734.2 seconds (145.6 minutes)
Average time per file: 3.02 seconds
================================================================================
```

**Note**: File numbers may appear out of order because workers complete at different speeds. This is normal and expected.

## Memory Usage

**Typical usage:**
- ~50-100 MB per worker process
- With 14 workers: ~700 MB - 1.4 GB total

**Peak usage (large files):**
- ~200-300 MB per worker
- With 14 workers: ~2.8-4.2 GB total

Ensure your system has adequate RAM. For systems with limited memory, reduce `--workers`.

## Error Handling

- Individual file failures don't stop overall processing
- Failed files reported as ERROR in output
- Other workers continue processing
- Cleanup happens even on failures (temp directories removed)

## Troubleshooting

### High Memory Usage

```bash
# Reduce number of workers
python3 convert_orderbook_to_1min_parallel.py --workers 4
```

### System Overload

If your system becomes unresponsive:
```bash
# Use fewer workers to leave CPU/RAM for other tasks
python3 convert_orderbook_to_1min_parallel.py --workers 8
```

### Debugging Individual Files

If you need to troubleshoot specific files, use the single-threaded version:
```bash
python3 convert_orderbook_to_1min.py --test
```

## Resuming Interrupted Processing

The parallel version has the same smart skip logic as the single-threaded version:
- Checks for existing output files before processing
- Skips files that already have all required outputs
- Can safely stop (Ctrl+C) and restart - will resume from where it left off

## Comparison with Single-Threaded Version

Both versions produce **identical output**. The choice is purely about speed vs. simplicity:

| Aspect | Single-Threaded | Parallel |
|--------|----------------|----------|
| **Output** | ✅ Identical | ✅ Identical |
| **Speed** | Baseline | 10-15x faster |
| **CPU Usage** | 1 core | All cores |
| **Memory** | Low | Higher |
| **Complexity** | Simple | More complex |
| **Debugging** | Easy | Harder |
| **Large Batches** | ⚠️ Very slow | ✅ Recommended |

## Technical Details

**Implementation:**
- Uses `concurrent.futures.ProcessPoolExecutor` for process pool management
- `multiprocessing` for true parallelism (bypasses Python's GIL)
- Each worker is a separate Python process
- Process-safe temp directory naming using PID
- Results collected via `as_completed()` iterator

**Why `multiprocessing` instead of `threading`?**
- Python's Global Interpreter Lock (GIL) prevents true parallel execution of threads
- This is a CPU-intensive task (decompression, parsing, sorting)
- `multiprocessing` creates separate processes that run in parallel on different cores
- Real performance gains: 10-15x speedup on multi-core systems

## License

Same as main project.

## See Also

- `README.md` - Main documentation (single-threaded version)
- `convert_orderbook_to_1min.py` - Single-threaded version
- `convert_orderbook_to_1min_parallel.py` - This parallel version


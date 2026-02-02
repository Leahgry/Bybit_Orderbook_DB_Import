#!/usr/bin/env python3
"""
Convert Bybit orderbook data (snapshot + delta) to 1-minute snapshot intervals.
PARALLEL VERSION - Uses multiprocessing for faster processing.

This script processes orderbook files containing snapshot and delta updates,
reconstructs the orderbook state, and extracts snapshots at exact 1-minute intervals.
Uses multiple CPU cores to process files in parallel.
"""

import json
import os
import zipfile
import gzip
import csv
import shutil
from pathlib import Path
from datetime import datetime, timezone
from collections import OrderedDict
from typing import Dict, List, Tuple, Optional
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count, Manager
import time

# Configuration
SOURCE_DIR = Path("/volumes/Data01/FinancialData/Bybit/OrderBook")
OUTPUT_DIR = Path("/volumes/Data01/FinancialData/Bybit/OrderBook_1m")
TEMP_DIR = Path("/tmp/orderbook_conversion_parallel")
INCLUDE_FORMATTED_TIMESTAMP = False  # Include human-readable timestamp column
MAX_WORKERS = cpu_count()  # Default: use all available CPU cores


class OrderbookState:
    """Maintains current orderbook state by applying snapshots and deltas."""
    
    def __init__(self, max_depth: int = None):
        self.bids: Dict[str, str] = OrderedDict()  # price -> quantity
        self.asks: Dict[str, str] = OrderedDict()  # price -> quantity
        self.max_depth = max_depth
        self.last_update_id = None
        self.last_timestamp = None
        
    def apply_snapshot(self, data: dict, timestamp: int):
        """Apply a snapshot to reset the orderbook state."""
        self.bids.clear()
        self.asks.clear()
        
        # Apply bids
        for price, qty in data.get('b', []):
            self.bids[price] = qty
            
        # Apply asks
        for price, qty in data.get('a', []):
            self.asks[price] = qty
            
        self.last_update_id = data.get('u')
        self.last_timestamp = timestamp
        
    def apply_delta(self, data: dict, timestamp: int):
        """Apply a delta update to modify the orderbook state."""
        # Update bids
        for price, qty in data.get('b', []):
            if qty == '0' or float(qty) == 0:
                self.bids.pop(price, None)  # Remove price level
            else:
                self.bids[price] = qty
                
        # Update asks
        for price, qty in data.get('a', []):
            if qty == '0' or float(qty) == 0:
                self.asks.pop(price, None)  # Remove price level
            else:
                self.asks[price] = qty
                
        self.last_update_id = data.get('u')
        self.last_timestamp = timestamp
        
    def get_snapshot(self, symbol: str, timestamp: int) -> Tuple[int, List[Tuple[str, str]], List[Tuple[str, str]]]:
        """Get current orderbook state as a snapshot.
        
        Returns:
            Tuple of (timestamp, bids_list, asks_list)
        """
        # Sort and limit depth
        sorted_bids = sorted(self.bids.items(), key=lambda x: float(x[0]), reverse=True)
        sorted_asks = sorted(self.asks.items(), key=lambda x: float(x[0]))
        
        if self.max_depth:
            sorted_bids = sorted_bids[:self.max_depth]
            sorted_asks = sorted_asks[:self.max_depth]
            
        return (timestamp, sorted_bids, sorted_asks)


def is_minute_boundary(timestamp_ms: int) -> bool:
    """Check if timestamp is at a 1-minute boundary (XX:XX:00.000)."""
    # Convert to datetime
    dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
    # Check if seconds and microseconds are zero
    return dt.second == 0 and dt.microsecond == 0


def get_minute_timestamp(timestamp_ms: int) -> int:
    """Get the previous minute boundary timestamp."""
    dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
    # Zero out seconds and microseconds
    minute_dt = dt.replace(second=0, microsecond=0)
    return int(minute_dt.timestamp() * 1000)


def format_timestamp(timestamp_ms: int) -> str:
    """
    Format Unix timestamp (milliseconds) to ISO 8601 format.
    
    Args:
        timestamp_ms: Unix timestamp in milliseconds
        
    Returns:
        Formatted timestamp string in 'yyyy-mm-ddThh:mm:ss.ssss' format
    """
    dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
    # Format with milliseconds (4 digits as specified)
    return dt.strftime('%Y-%m-%dT%H:%M:%S') + f'.{timestamp_ms % 1000:04d}'


def get_output_path(archive_filename: str, pair_name: str, output_base_dir: Path, depth: int) -> Path:
    """
    Generate output path for 1-minute CSV file.
    
    Args:
        archive_filename: Original archive filename (e.g., "2025-10-25_BTCUSDT_ob200.data.zip")
        pair_name: Trading pair name (e.g., "BTCUSDT")
        output_base_dir: Base output directory
        depth: Orderbook depth (200 or 500)
        
    Returns:
        Path object for output CSV file
    """
    # Remove archive extensions and create output filename
    base_name = archive_filename.replace('.zip', '').replace('.gz', '').replace('.data', '')
    
    # Create output filename: base_name with depth and _1m suffix
    # e.g., "2025-10-25_BTCUSDT_ob200_1m.csv"
    output_filename = f"{base_name.rsplit('_', 1)[0]}_ob{depth}_1m.csv"
    
    # Create path: output_base/0200_or_0500/PAIR/filename.csv
    depth_dir = f"0{depth}"
    output_path = output_base_dir / depth_dir / pair_name / output_filename
    
    return output_path


def output_file_exists(archive_filename: str, pair_name: str, output_base_dir: Path, depth: int) -> bool:
    """Check if output file already exists for given parameters."""
    output_path = get_output_path(archive_filename, pair_name, output_base_dir, depth)
    return output_path.exists()


def extract_archive(archive_path: Path, temp_dir: Path) -> Optional[Path]:
    """Extract archive file to temporary directory and return path to extracted .data file."""
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
            
        # Find the extracted .data file
        data_files = list(temp_dir.glob('*.data'))
        if data_files:
            return data_files[0]
        return None
    except Exception as e:
        print(f"Error extracting {archive_path}: {e}")
        return None


def process_orderbook_file(
    data_file: Path, 
    output_path: Path,
    max_depth: int = None,
    include_formatted_timestamp: bool = False
) -> int:
    """
    Process a single orderbook data file and generate 1-minute snapshots.
    
    Args:
        data_file: Path to extracted .data file
        output_path: Path where output CSV should be written
        max_depth: Maximum orderbook depth to output (None = unlimited)
        include_formatted_timestamp: Whether to include formatted timestamp column
        
    Returns:
        Number of snapshots generated
    """
    orderbook = OrderbookState(max_depth=max_depth)
    snapshots_written = 0
    last_minute = None
    symbol = data_file.stem.split('_')[1] if '_' in data_file.stem else 'UNKNOWN'
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Open output file for writing
    with open(output_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Determine header based on depth
        depth = max_depth if max_depth else 500  # Default to 500 if not specified
        
        # Build header
        header = []
        if include_formatted_timestamp:
            header.append('timestamp_formatted')
        header.append('timestamp')
        
        for i in range(1, depth + 1):
            header.extend([f'bid_price_{i}', f'bid_qty_{i}'])
        for i in range(1, depth + 1):
            header.extend([f'ask_price_{i}', f'ask_qty_{i}'])
            
        writer.writerow(header)
        
        # Process the data file line by line
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
                    
                    if msg_type == 'snapshot':
                        orderbook.apply_snapshot(data, timestamp)
                    elif msg_type == 'delta':
                        orderbook.apply_delta(data, timestamp)
                    
                    # Check if we've crossed a minute boundary
                    current_minute = get_minute_timestamp(timestamp)
                    
                    if last_minute is not None and current_minute > last_minute:
                        # Write snapshot for the previous minute boundary
                        ts, bids, asks = orderbook.get_snapshot(symbol, last_minute)
                        
                        # Build row
                        row = []
                        if include_formatted_timestamp:
                            row.append(format_timestamp(ts))
                        row.append(ts)
                        
                        # Add bids (price, qty pairs)
                        for price, qty in bids:
                            row.extend([price, qty])
                        # Pad if we have fewer bids than max_depth
                        for _ in range(depth - len(bids)):
                            row.extend(['', ''])
                            
                        # Add asks (price, qty pairs)
                        for price, qty in asks:
                            row.extend([price, qty])
                        # Pad if we have fewer asks than max_depth
                        for _ in range(depth - len(asks)):
                            row.extend(['', ''])
                            
                        writer.writerow(row)
                        snapshots_written += 1
                    
                    last_minute = current_minute
                    
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    continue
    
    return snapshots_written


def process_single_file(args_tuple):
    """
    Process a single archive file. This function is designed to be called by ProcessPoolExecutor.
    
    Args:
        args_tuple: Tuple containing (archive_file, pair_name, source_depth, output_depths, 
                    output_base_dir, temp_base_dir, include_formatted_timestamp, file_num, total_files)
    
    Returns:
        Tuple of (success: bool, message: str, file_num: int, snapshots_generated: list)
    """
    (archive_file, pair_name, source_depth, output_depths, 
     output_base_dir, temp_base_dir, include_formatted_timestamp, file_num, total_files) = args_tuple
    
    # Create unique temp directory for this file using process ID
    import os
    pid = os.getpid()
    temp_dir = temp_base_dir / f"temp_{pid}_{archive_file.stem}"
    
    try:
        # Extract archive
        data_file = extract_archive(archive_file, temp_dir)
        if not data_file:
            return (False, f"Failed to extract", file_num, [])
        
        snapshots_generated = []
        
        # Process for each required output depth
        for depth in output_depths:
            output_path = get_output_path(archive_file.name, pair_name, output_base_dir, depth)
            
            # Skip if already exists
            if output_path.exists():
                continue
            
            # Process the file
            num_snapshots = process_orderbook_file(
                data_file,
                output_path,
                max_depth=depth,
                include_formatted_timestamp=include_formatted_timestamp
            )
            snapshots_generated.append(num_snapshots)
        
        # Determine status and depths string
        if len(snapshots_generated) == 0:
            status = "SKIPPED"
            depths_str = f"[{','.join(map(str, output_depths))}]"
        else:
            status = "DONE"
            depths_str = f"[{','.join(map(str, output_depths))}]"
        
        return (True, status, file_num, snapshots_generated, depths_str, pair_name, archive_file.name)
        
    except Exception as e:
        return (False, f"ERROR: {str(e)[:50]}", file_num, [], "", pair_name, archive_file.name)
        
    finally:
        # Clean up temp directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


def process_directory(
    source_dir: Path,
    output_dir: Path,
    temp_dir: Path,
    include_formatted_timestamp: bool = False,
    max_workers: int = MAX_WORKERS
):
    """
    Process all orderbook files in the source directory using parallel processing.
    
    Args:
        source_dir: Source directory containing 0200/ and 0500/ subdirectories
        output_dir: Output directory for 1-minute CSVs
        temp_dir: Temporary directory for extraction
        include_formatted_timestamp: Whether to include formatted timestamp column
        max_workers: Maximum number of parallel worker processes
    """
    print(f"Processing with {max_workers} parallel workers")
    print("Scanning source directories...")
    
    # First pass: collect all files to process
    files_to_process = []
    
    for depth_dir in ['0200', '0500']:
        depth_source_dir = source_dir / depth_dir
        if not depth_source_dir.exists():
            continue
        
        source_depth = 200 if depth_dir == '0200' else 500
        
        # Determine which output depths to generate based on source directory
        if source_depth == 200:
            output_depths = [200]  # Only generate 200-level from 200-level source
        else:
            output_depths = [200, 500]  # Generate both from 500-level source
        
        # Iterate through pair directories
        for pair_dir in depth_source_dir.iterdir():
            if not pair_dir.is_dir():
                continue
            
            pair_name = pair_dir.name
            
            # Find all archive files in pair directory
            archive_files = list(pair_dir.glob('*.zip')) + list(pair_dir.glob('*.gz'))
            
            for archive_file in archive_files:
                # Check which output files need to be created
                needs_processing = False
                depths_to_create = []
                
                for depth in output_depths:
                    if not output_file_exists(archive_file.name, pair_name, output_dir, depth):
                        needs_processing = True
                        depths_to_create.append(depth)
                
                # Only add to processing queue if at least one output is needed
                if needs_processing:
                    files_to_process.append((
                        archive_file,
                        pair_name,
                        source_depth,
                        depths_to_create,
                        output_dir,
                        temp_dir,
                        include_formatted_timestamp
                    ))
    
    total_files = len(files_to_process)
    print(f"Found {total_files} archive files to process\n")
    
    if total_files == 0:
        print("No files to process (all outputs already exist)")
        return
    
    # Add file numbers to arguments
    files_with_nums = [
        (*file_args, idx + 1, total_files) 
        for idx, file_args in enumerate(files_to_process)
    ]
    
    # Process files in parallel
    start_time = time.time()
    completed = 0
    errors = 0
    skipped = 0
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {executor.submit(process_single_file, args): args for args in files_with_nums}
        
        # Process results as they complete
        for future in as_completed(futures):
            try:
                result = future.result()
                success, status_or_msg, file_num, snapshots, *extra = result
                
                if success:
                    depths_str, pair_name, filename = extra
                    if status_or_msg == "SKIPPED":
                        skipped += 1
                        print(f"[{file_num}/{total_files}] SKIPPED  {pair_name:10} {filename:40} {depths_str}")
                    else:
                        completed += 1
                        snapshot_str = f"{snapshots[0]} snapshots" if snapshots else "0 snapshots"
                        print(f"[{file_num}/{total_files}] DONE     {pair_name:10} {filename:40} {depths_str} → {snapshot_str} ✓")
                else:
                    errors += 1
                    _, pair_name, filename = extra if len(extra) >= 3 else ("", "", "")
                    print(f"[{file_num}/{total_files}] ERROR    {pair_name:10} {filename:40} → {status_or_msg}")
                    
            except Exception as e:
                errors += 1
                print(f"Task failed with exception: {str(e)[:100]}")
    
    elapsed = time.time() - start_time
    
    # Print summary
    print("\n" + "="*80)
    print(f"Processing complete!")
    print(f"Total files: {total_files}")
    print(f"Successfully processed: {completed}")
    print(f"Skipped (already exist): {skipped}")
    print(f"Errors: {errors}")
    print(f"Time elapsed: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    if completed > 0:
        print(f"Average time per file: {elapsed/completed:.2f} seconds")
    print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Convert Bybit orderbook snapshot+delta data to 1-minute snapshots (PARALLEL VERSION)'
    )
    parser.add_argument(
        '--source-dir',
        type=Path,
        default=SOURCE_DIR,
        help='Source directory containing orderbook data'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=OUTPUT_DIR,
        help='Output directory for 1-minute CSV files'
    )
    parser.add_argument(
        '--temp-dir',
        type=Path,
        default=TEMP_DIR,
        help='Temporary directory for extraction'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=MAX_WORKERS,
        help=f'Number of parallel workers (default: {MAX_WORKERS} = all CPU cores)'
    )
    parser.add_argument(
        '--include-formatted-timestamp',
        action='store_true',
        default=INCLUDE_FORMATTED_TIMESTAMP,
        help='Include human-readable timestamp column in output'
    )
    parser.add_argument(
        '--no-formatted-timestamp',
        action='store_false',
        dest='include_formatted_timestamp',
        help='Exclude human-readable timestamp column from output (default)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run in test mode with sample files'
    )
    
    args = parser.parse_args()
    
    if args.test:
        print("Running in TEST mode")
        print("="*80)
        
        # Test with sample files (if they exist)
        test_files = [
            ('2025-10-25_MNTUSDT_ob200.data.zip', 'MNTUSDT', 200, [200]),
            ('2025-10-25_ENAUSDT_ob500.data.zip', 'ENAUSDT', 500, [200, 500]),
        ]
        
        for test_file, pair, source_depth, output_depths in test_files:
            test_path = Path(test_file)
            if not test_path.exists():
                print(f"Test file not found: {test_file}")
                continue
            
            print(f"\nProcessing: {test_file}")
            print(f"  Pair: {pair}")
            print(f"  Source depth: {source_depth}")
            print(f"  Output depths: {output_depths}")
            
            # Create test temp directory
            test_temp = args.temp_dir / 'test_temp'
            test_temp.mkdir(parents=True, exist_ok=True)
            
            # Extract
            data_file = extract_archive(test_path, test_temp)
            if not data_file:
                print(f"  Failed to extract {test_file}")
                continue
            
            # Process for each depth
            for depth in output_depths:
                output_path = get_output_path(test_file, pair, args.output_dir, depth)
                print(f"  Processing depth {depth}...")
                
                num_snapshots = process_orderbook_file(
                    data_file,
                    output_path,
                    max_depth=depth,
                    include_formatted_timestamp=args.include_formatted_timestamp
                )
                
                file_size = output_path.stat().st_size / 1024  # KB
                print(f"    Output: {output_path}")
                print(f"    Snapshots: {num_snapshots}")
                print(f"    Size: {file_size:.1f} KB")
            
            # Cleanup test temp
            shutil.rmtree(test_temp, ignore_errors=True)
        
        print("\n" + "="*80)
        print("Test complete")
        
    else:
        # Production mode
        print("="*80)
        print("Bybit Orderbook to 1-Minute Converter (PARALLEL VERSION)")
        print("="*80)
        print(f"Source: {args.source_dir}")
        print(f"Output: {args.output_dir}")
        print(f"Temp: {args.temp_dir}")
        print(f"Workers: {args.workers}")
        print(f"Formatted timestamp: {'Yes' if args.include_formatted_timestamp else 'No'}")
        print("="*80 + "\n")
        
        process_directory(
            args.source_dir,
            args.output_dir,
            args.temp_dir,
            args.include_formatted_timestamp,
            args.workers
        )


if __name__ == '__main__':
    main()


#!/usr/bin/env python3
"""
Convert Bybit orderbook data (snapshot + delta) to 1-minute snapshot intervals.

This script processes orderbook files containing snapshot and delta updates,
reconstructs the orderbook state, and extracts snapshots at exact 1-minute intervals.
"""

import json
import os
import zipfile
import gzip
import csv
from pathlib import Path
from datetime import datetime, timezone
from collections import OrderedDict
from typing import Dict, List, Tuple
import argparse

# Configuration
SOURCE_DIR = Path("/volumes/Data01/FinancialData/Bybit/OrderBook")
OUTPUT_DIR = Path("/volumes/Data01/FinancialData/Bybit/OrderBook_1m")
TEMP_DIR = Path("/tmp/orderbook_conversion")
INCLUDE_FORMATTED_TIMESTAMP = False  # Include human-readable timestamp column


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
    dt = dt.replace(second=0, microsecond=0)
    return int(dt.timestamp() * 1000)


def format_timestamp(timestamp_ms: int) -> str:
    """
    Format Unix timestamp (milliseconds) to ISO 8601 format.
    
    Args:
        timestamp_ms: Unix timestamp in milliseconds
        
    Returns:
        Formatted string in yyyy-mm-ddThh:mm:ss.ssss format
    """
    dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
    # Format with milliseconds (4 digits)
    return dt.strftime('%Y-%m-%dT%H:%M:%S') + f'.{timestamp_ms % 1000:04d}'


def process_orderbook_file(input_path: Path, output_path: Path, max_depth: int = None, include_formatted_timestamp: bool = True, quiet: bool = False):
    """
    Process a single orderbook file and extract 1-minute snapshots.
    
    Args:
        input_path: Path to input .data or .csv file (may be compressed)
        output_path: Path to output CSV file
        max_depth: Maximum orderbook depth (None for unlimited)
        include_formatted_timestamp: Include human-readable timestamp column
        quiet: Suppress progress output
    """
    if not quiet:
        print(f"Processing: {input_path.name}")
    
    orderbook = OrderbookState(max_depth=max_depth)
    minute_snapshots = []
    last_minute_ts = None
    symbol = None
    actual_max_depth = 0  # Track actual max depth encountered
    
    # Determine if file is compressed
    is_gzipped = input_path.suffix == '.gz'
    
    # Open file (handle both compressed and uncompressed)
    if is_gzipped:
        file_handle = gzip.open(input_path, 'rt', encoding='utf-8')
    else:
        file_handle = open(input_path, 'r', encoding='utf-8')
    
    try:
        line_count = 0
        for line in file_handle:
            line = line.strip()
            if not line:
                continue
                
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                print(f"Warning: Could not parse line {line_count + 1}")
                continue
                
            line_count += 1
            
            # Extract data
            msg_type = msg.get('type')
            timestamp = msg.get('ts')
            data = msg.get('data', {})
            
            if not symbol:
                symbol = data.get('s')
            
            # Apply update to orderbook state
            if msg_type == 'snapshot':
                orderbook.apply_snapshot(data, timestamp)
            elif msg_type == 'delta':
                orderbook.apply_delta(data, timestamp)
            else:
                continue
                
            # Check if we've crossed a minute boundary
            minute_ts = get_minute_timestamp(timestamp)
            
            # If this is a new minute boundary, capture the snapshot
            if last_minute_ts != minute_ts:
                if last_minute_ts is not None:
                    # Capture snapshot at the previous minute boundary
                    ts, bids, asks = orderbook.get_snapshot(symbol, last_minute_ts)
                    minute_snapshots.append((ts, bids, asks))
                    # Track max depth
                    actual_max_depth = max(actual_max_depth, len(bids), len(asks))
                    
                last_minute_ts = minute_ts
        
        # Capture final minute snapshot
        if last_minute_ts is not None and orderbook.last_timestamp:
            ts, bids, asks = orderbook.get_snapshot(symbol, last_minute_ts)
            minute_snapshots.append((ts, bids, asks))
            actual_max_depth = max(actual_max_depth, len(bids), len(asks))
            
    finally:
        file_handle.close()
    
    # Write output as CSV
    if minute_snapshots:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Determine depth for CSV columns (use the actual max depth or configured max)
        csv_depth = max_depth if max_depth else actual_max_depth
        
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            
            # Write header
            header = []
            if include_formatted_timestamp:
                header.append('timestamp_formatted')
            header.append('timestamp')
            for i in range(1, csv_depth + 1):
                header.extend([f'bid_price_{i}', f'bid_qty_{i}'])
            for i in range(1, csv_depth + 1):
                header.extend([f'ask_price_{i}', f'ask_qty_{i}'])
            writer.writerow(header)
            
            # Write data rows
            for timestamp, bids, asks in minute_snapshots:
                row = []
                if include_formatted_timestamp:
                    row.append(format_timestamp(timestamp))
                row.append(timestamp)
                
                # Add bid data
                for i in range(csv_depth):
                    if i < len(bids):
                        row.extend([bids[i][0], bids[i][1]])  # price, qty
                    else:
                        row.extend(['', ''])  # Empty if not enough levels
                
                # Add ask data
                for i in range(csv_depth):
                    if i < len(asks):
                        row.extend([asks[i][0], asks[i][1]])  # price, qty
                    else:
                        row.extend(['', ''])  # Empty if not enough levels
                
                writer.writerow(row)
        
        if not quiet:
            print(f"  -> Extracted {len(minute_snapshots)} 1-minute snapshots")
            print(f"  -> CSV depth: {csv_depth} levels")
            print(f"  -> Output: {output_path}")
        
        return len(minute_snapshots)
    else:
        if not quiet:
            print(f"  -> No snapshots extracted")
        return 0


def get_output_path(archive_filename: str, pair_name: str, output_base_dir: Path, depth: int) -> Path:
    """
    Generate output path for 1-minute CSV file.
    
    Args:
        archive_filename: Original archive filename (e.g., "2025-10-25_MNTUSDT_ob200.data.zip")
        pair_name: Trading pair name (e.g., "MNTUSDT")
        output_base_dir: Base output directory
        depth: Orderbook depth (200 or 500)
    
    Returns:
        Path to output CSV file
    """
    # Extract date and pair from filename
    # Format: YYYY-MM-DD_PAIR_obXXX.data.zip
    base_name = archive_filename.replace('.zip', '').replace('.gz', '').replace('.data', '')
    
    # Create output filename with depth and _1m suffix
    output_filename = f"{base_name.rsplit('_', 1)[0]}_ob{depth}_1m.csv"
    
    # Build output path: output_base_dir/0XXX/PAIR/filename.csv
    depth_dir = f"0{depth}"
    output_path = output_base_dir / depth_dir / pair_name / output_filename
    
    return output_path


def output_file_exists(archive_filename: str, pair_name: str, output_base_dir: Path, depth: int) -> bool:
    """Check if output file already exists for this archive and depth."""
    output_path = get_output_path(archive_filename, pair_name, output_base_dir, depth)
    return output_path.exists()


def extract_archive(archive_path: Path, extract_dir: Path) -> List[Path]:
    """Extract archive and return list of extracted files."""
    extracted_files = []
    
    if archive_path.suffix == '.zip':
        with zipfile.ZipFile(archive_path, 'r') as zf:
            zf.extractall(extract_dir)
            extracted_files = [extract_dir / name for name in zf.namelist()]
    elif archive_path.suffix == '.gz':
        output_path = extract_dir / archive_path.stem
        with gzip.open(archive_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                f_out.write(f_in.read())
        extracted_files = [output_path]
    else:
        # Not compressed, just return the path
        extracted_files = [archive_path]
        
    return extracted_files


def process_directory(source_dir: Path, output_dir: Path, temp_base_dir: Path, include_formatted_timestamp: bool = True):
    """
    Process all orderbook files in directory structure.
    
    Creates both 200-level and 500-level 1-minute snapshots for each file.
    
    Args:
        source_dir: Source directory containing 0200/ and 0500/ subdirectories
        output_dir: Output directory for 1-minute snapshots
        temp_base_dir: Base directory for temporary file extraction
        include_formatted_timestamp: Include human-readable timestamp column
    """
    import shutil
    
    # Ensure temp directory exists
    temp_base_dir.mkdir(parents=True, exist_ok=True)
    
    total_processed = 0
    total_skipped = 0
    
    # First pass: count total files to process
    print("Scanning source directories...")
    total_files = 0
    for depth_dir in ['0200', '0500']:
        depth_path = source_dir / depth_dir
        if depth_path.exists():
            for pair_dir in depth_path.iterdir():
                if pair_dir.is_dir():
                    archive_files = list(pair_dir.glob('*.zip')) + list(pair_dir.glob('*.gz'))
                    total_files += len(archive_files)
    
    print(f"Found {total_files} archive files to process\n")
    current_file = 0
    
    # Process both 200 and 500 level directories
    for depth_dir in ['0200', '0500']:
        depth_path = source_dir / depth_dir
        
        if not depth_path.exists():
            print(f"Warning: {depth_path} does not exist, skipping")
            continue
            
        # Determine which output depths to generate based on source directory
        source_depth = 200 if depth_dir == '0200' else 500
        if source_depth == 200:
            output_depths = [200]  # Only generate 200-level from 200-level source
        else:
            output_depths = [200, 500]  # Generate both from 500-level source
        
        # Iterate through pair directories
        for pair_dir in sorted(depth_path.iterdir()):
            if not pair_dir.is_dir():
                continue
                
            pair_name = pair_dir.name
            
            # Process all archive files in this directory
            archive_files = sorted(pair_dir.glob('*.zip')) + sorted(pair_dir.glob('*.gz'))
            
            for archive_file in archive_files:
                current_file += 1
                
                # Check if output files already exist for required depths
                exists_checks = {depth: output_file_exists(archive_file.name, pair_name, output_dir, depth) 
                                for depth in output_depths}
                
                # Skip if all required outputs exist
                if all(exists_checks.values()):
                    status = "SKIP"
                    depths_str = f"[{','.join(map(str, output_depths))}]"
                    print(f"[{current_file}/{total_files}] {status:8} {pair_name:10} {archive_file.name:40} {depths_str} (exists)")
                    total_skipped += 1
                    continue
                
                # Determine what needs to be generated
                to_generate = [d for d in output_depths if not exists_checks[d]]
                depths_str = f"[{','.join(map(str, to_generate))}]"
                print(f"[{current_file}/{total_files}] PROCESS  {pair_name:10} {archive_file.name:40} {depths_str}", end='', flush=True)
                
                # Create unique temp directory for this file
                temp_dir = temp_base_dir / f"extract_{os.getpid()}_{archive_file.stem}"
                temp_dir.mkdir(parents=True, exist_ok=True)
                
                try:
                    # Extract archive to temp directory
                    extracted_files = extract_archive(archive_file, temp_dir)
                    
                    snapshots_generated = []
                    for extracted_file in extracted_files:
                        # Process for required output depths only
                        for depth in output_depths:
                            # Skip if already exists
                            if exists_checks[depth]:
                                continue
                            
                            # Get output path
                            output_path = get_output_path(
                                archive_file.name,
                                pair_name,
                                output_dir,
                                depth
                            )
                            
                            # Ensure output directory exists
                            output_path.parent.mkdir(parents=True, exist_ok=True)
                            
                            # Process the file (quietly)
                            snapshot_count = process_orderbook_file(
                                extracted_file,
                                output_path,
                                max_depth=depth,
                                include_formatted_timestamp=include_formatted_timestamp,
                                quiet=True
                            )
                            
                            if snapshot_count > 0:
                                total_processed += 1
                                snapshots_generated.append(snapshot_count)
                    
                    # Print completion status on same line
                    if snapshots_generated:
                        print(f" → {snapshots_generated[0]} snapshots ✓")
                    else:
                        print(f" → ERROR")
                                
                finally:
                    # Clean up temp directory
                    shutil.rmtree(temp_dir, ignore_errors=True)
    
    print(f"\n{'='*80}")
    print(f"Summary:")
    print(f"  Files processed: {total_processed}")
    print(f"  Files skipped: {total_skipped}")
    print(f"{'='*80}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Convert orderbook snapshot+delta data to 1-minute snapshots. '
                    '200-level sources generate 200-level outputs only. '
                    '500-level sources generate both 200-level and 500-level outputs.'
    )
    parser.add_argument(
        '--source-dir',
        type=Path,
        default=SOURCE_DIR,
        help=f'Source directory (default: {SOURCE_DIR})'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=OUTPUT_DIR,
        help=f'Output directory (default: {OUTPUT_DIR})'
    )
    parser.add_argument(
        '--temp-dir',
        type=Path,
        default=TEMP_DIR,
        help=f'Temporary directory for extraction (default: {TEMP_DIR})'
    )
    parser.add_argument(
        '--include-formatted-timestamp',
        action='store_true',
        default=INCLUDE_FORMATTED_TIMESTAMP,
        help='Include human-readable timestamp column (default: True)'
    )
    parser.add_argument(
        '--no-formatted-timestamp',
        dest='include_formatted_timestamp',
        action='store_false',
        help='Exclude human-readable timestamp column'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode: process test files in project root'
    )
    
    args = parser.parse_args()
    
    if args.test:
        # Test mode: process files in project root
        print("Running in TEST mode")
        print(f"Formatted timestamp: {args.include_formatted_timestamp}")
        print("\nOutput strategy:")
        print("  - ob200 source -> 200-level output only")
        print("  - ob500 source -> both 200-level and 500-level outputs\n")
        
        import shutil
        
        test_dir = Path("/Users/andy/Documents/Projects/Bybit_Orderbook_Reformat")
        output_dir = test_dir / "test_output"
        temp_dir = test_dir / "test_temp"
        
        # Clean and create directories
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Process test files
        test_files = [
            (test_dir / "2025-10-25_MNTUSDT_ob200.data.zip", 200, [200]),  # 200-level source -> 200-level output only
            (test_dir / "2025-08-19_MNTUSDT_ob500.data.zip", 500, [200, 500])  # 500-level source -> both outputs
        ]
        
        for test_file, source_depth, output_depths in test_files:
            if not test_file.exists():
                print(f"Warning: {test_file} not found, skipping")
                continue
            
            print(f"\nProcessing: {test_file.name} (source depth: {source_depth})")
            print(f"  Will generate: {output_depths}-level outputs")
            
            # Extract to temp
            extract_dir = temp_dir / test_file.stem
            extract_dir.mkdir(parents=True, exist_ok=True)
            
            try:
                extracted_files = extract_archive(test_file, extract_dir)
                
                for extracted_file in extracted_files:
                    # Process only for the appropriate depths based on source
                    for depth in output_depths:
                        depth_dir = output_dir / f"0{depth}" / "MNTUSDT"
                        depth_dir.mkdir(parents=True, exist_ok=True)
                        
                        output_filename = f"{extracted_file.stem}_ob{depth}_1min.csv"
                        output_path = depth_dir / output_filename
                        
                        print(f"  -> Generating {depth}-level snapshots...")
                        process_orderbook_file(
                            extracted_file,
                            output_path,
                            max_depth=depth,
                            include_formatted_timestamp=args.include_formatted_timestamp
                        )
                        
            finally:
                shutil.rmtree(extract_dir, ignore_errors=True)
        
        # Clean up temp directory
        shutil.rmtree(temp_dir, ignore_errors=True)
        
    else:
        # Production mode
        print("Processing orderbook files")
        print(f"Source: {args.source_dir}")
        print(f"Output: {args.output_dir}")
        print(f"Temp:   {args.temp_dir}")
        print(f"Formatted timestamp: {args.include_formatted_timestamp}")
        print("\nOutput strategy:")
        print("  - 0200 sources -> 200-level outputs only")
        print("  - 0500 sources -> both 200-level and 500-level outputs\n")
        
        process_directory(
            args.source_dir,
            args.output_dir,
            args.temp_dir,
            include_formatted_timestamp=args.include_formatted_timestamp
        )
    
    print("\n" + "="*80)
    print("Processing complete!")
    print("="*80)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Video Database Query Tool

Utility for querying video file database with convenient formatting.

Usage:
python video_query.py --export-duplicates duplicates_by_hash.txt --export-pattern 'Camera Uploads'

python video_query.py --export-list high_quality_files.txt --export-min-bitrate 15 --export-min-size 50
"""

import sqlite3
import argparse
import datetime
import hashlib
import os
from colorama import Fore, Style, init

# Инициализация colorama
init()

def format_file_size(bytes_size):
    """Formats file size in human readable format"""
    if bytes_size is None:
        return "N/A"
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} PB"

def format_bitrate(bitrate):
    """Formats bitrate in human readable format"""
    if bitrate is None or bitrate == 0:
        return "N/A"
    
    # Convert from bits/sec to Mbit/s
    mbit_rate = bitrate / 1_000_000
    
    if mbit_rate < 1:
        kbit_rate = bitrate / 1_000
        return f"{kbit_rate:.1f} kbit/s"
    elif mbit_rate < 1000:
        return f"{mbit_rate:.1f} Mbit/s"
    else:
        gbit_rate = mbit_rate / 1000
        return f"{gbit_rate:.1f} Gbit/s"

def format_duration(duration):
    """Formats duration in human readable format"""
    if duration is None or duration == 0:
        return "N/A"
    
    hours = int(duration // 3600)
    minutes = int((duration % 3600) // 60)
    seconds = int(duration % 60)
    
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes:02d}:{seconds:02d}"

def query_largest_files(db_path, limit=20):
    """Shows the largest files"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = '''
        SELECT 
            file_path,
            file_name,
            file_size,
            duration,
            bit_rate,
            width || 'x' || height as resolution,
            codec_name,
            is_corrupted
        FROM videos 
        WHERE file_size IS NOT NULL
        ORDER BY file_size DESC 
        LIMIT ?
    '''
    
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    
    print(f"\n{Fore.CYAN}🗂️  {limit} LARGEST FILES{Style.RESET_ALL}")
    print("=" * 120)
    
    # Table header
    header = f"{'#':<3} {'Size':<10} {'Duration':<8} {'Bitrate':<12} {'Resolution':<10} {'Codec':<8} {'Status':<6} {'File'}"
    print(f"{Fore.YELLOW}{header}{Style.RESET_ALL}")
    print("-" * 120)
    
    for i, row in enumerate(results, 1):
        file_path, file_name, file_size, duration, bit_rate, resolution, codec_name, is_corrupted = row
        
        # Format data
        size_str = format_file_size(file_size)
        duration_str = format_duration(duration)
        bitrate_str = format_bitrate(bit_rate)
        codec_str = codec_name[:7] if codec_name else "N/A"
        status_str = "❌BAD" if is_corrupted else "✅OK"
        
        # Color highlighting
        status_color = Fore.RED if is_corrupted else Fore.GREEN
        size_color = Fore.MAGENTA if file_size and file_size > 1_000_000_000 else Fore.BLUE  # > 1GB
        
        print(f"{i:<3} {size_color}{size_str:<10}{Style.RESET_ALL} {duration_str:<8} {bitrate_str:<12} "
              f"{resolution:<10} {codec_str:<8} {status_color}{status_str:<6}{Style.RESET_ALL} {file_name}")
    
    conn.close()

def query_high_bitrate_files(db_path, min_bitrate_mbps=10, limit=20):
    """Shows files with high bitrate"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    min_bitrate_bps = min_bitrate_mbps * 1_000_000  # Convert Mbps to bps
    
    query = '''
        SELECT 
            file_path,
            file_name,
            file_size,
            duration,
            bit_rate,
            width || 'x' || height as resolution,
            codec_name,
            is_corrupted
        FROM videos 
        WHERE bit_rate IS NOT NULL AND bit_rate >= ? AND is_corrupted = 0
        ORDER BY bit_rate DESC 
        LIMIT ?
    '''
    
    cursor.execute(query, (min_bitrate_bps, limit))
    results = cursor.fetchall()
    
    print(f"\n{Fore.CYAN}⚡ HIGH BITRATE FILES (≥{min_bitrate_mbps} Mbit/s){Style.RESET_ALL}")
    print("=" * 120)
    
    # Table header
    header = f"{'#':<3} {'Bitrate':<12} {'Size':<10} {'Duration':<8} {'Resolution':<10} {'Codec':<8} {'File'}"
    print(f"{Fore.YELLOW}{header}{Style.RESET_ALL}")
    print("-" * 120)
    
    for i, row in enumerate(results, 1):
        file_path, file_name, file_size, duration, bit_rate, resolution, codec_name, is_corrupted = row
        
        # Format data
        bitrate_str = format_bitrate(bit_rate)
        size_str = format_file_size(file_size)
        duration_str = format_duration(duration)
        codec_str = codec_name[:7] if codec_name else "N/A"
        
        # Color highlighting for very high bitrate
        bitrate_color = Fore.RED if bit_rate and bit_rate > 50_000_000 else Fore.MAGENTA  # > 50 Mbps
        
        print(f"{i:<3} {bitrate_color}{bitrate_str:<12}{Style.RESET_ALL} {size_str:<10} {duration_str:<8} "
              f"{resolution:<10} {codec_str:<8} {file_name}")
    
    conn.close()

def query_longest_files(db_path, limit=20):
    """Shows the longest files"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = '''
        SELECT 
            file_path,
            file_name,
            file_size,
            duration,
            bit_rate,
            width || 'x' || height as resolution,
            codec_name,
            is_corrupted
        FROM videos 
        WHERE duration IS NOT NULL AND duration > 0 AND is_corrupted = 0
        ORDER BY duration DESC 
        LIMIT ?
    '''
    
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    
    print(f"\n{Fore.CYAN}⏱️  {limit} LONGEST FILES{Style.RESET_ALL}")
    print("=" * 120)
    
    # Table header
    header = f"{'#':<3} {'Duration':<10} {'Size':<10} {'Bitrate':<12} {'Resolution':<10} {'Codec':<8} {'File'}"
    print(f"{Fore.YELLOW}{header}{Style.RESET_ALL}")
    print("-" * 120)
    
    for i, row in enumerate(results, 1):
        file_path, file_name, file_size, duration, bit_rate, resolution, codec_name, is_corrupted = row
        
        # Format data
        duration_str = format_duration(duration)
        size_str = format_file_size(file_size)
        bitrate_str = format_bitrate(bit_rate)
        codec_str = codec_name[:7] if codec_name else "N/A"
        
        # Color highlighting for very long files
        duration_color = Fore.RED if duration and duration > 3600 else Fore.CYAN  # > 1 hour
        
        print(f"{i:<3} {duration_color}{duration_str:<10}{Style.RESET_ALL} {size_str:<10} {bitrate_str:<12} "
              f"{resolution:<10} {codec_str:<8} {file_name}")
    
    conn.close()

def export_files_list(db_path, output_file, min_bitrate_mbps=15, min_size_mb=50, short_format=False):
    """Exports list of files by given criteria to text file"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    min_bitrate_bps = min_bitrate_mbps * 1_000_000  # Convert Mbps to bps
    min_size_bytes = min_size_mb * 1024 * 1024     # Convert MB to bytes
    
    query = '''
        SELECT 
            file_path,
            file_name,
            file_size,
            bit_rate,
            duration,
            width || 'x' || height as resolution,
            codec_name
        FROM videos 
        WHERE bit_rate IS NOT NULL 
          AND bit_rate >= ? 
          AND file_size IS NOT NULL 
          AND file_size >= ?
          AND is_corrupted = 0
        ORDER BY file_path ASC
    '''
    
    cursor.execute(query, (min_bitrate_bps, min_size_bytes))
    results = cursor.fetchall()
    
    if not results:
        print(f"{Fore.YELLOW}No files found with bitrate ≥{min_bitrate_mbps} Mbit/s and size ≥{min_size_mb} MB{Style.RESET_ALL}")
        conn.close()
        return
    
    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        total_size = 0
        total_duration = 0
        
        if not short_format:
            # Header for full format
            f.write(f"# List of video files with bitrate ≥{min_bitrate_mbps} Mbit/s and size ≥{min_size_mb} MB\n")
            f.write(f"# Found {len(results)} files\n")
            f.write(f"# Created: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("#\n")
            f.write("# Format: file_path | size | bitrate | duration | resolution | codec\n")
            f.write("#" + "="*100 + "\n\n")
        
        for row in results:
            file_path, file_name, file_size, bit_rate, duration, resolution, codec_name = row
            
            total_size += file_size if file_size else 0
            total_duration += duration if duration else 0
            
            if short_format:
                # Short format: only file paths
                f.write(f"{file_path}\n")
            else:
                # Full format: file path with metadata
                size_str = format_file_size(file_size)
                bitrate_str = format_bitrate(bit_rate)
                duration_str = format_duration(duration) if duration else "N/A"
                codec_str = codec_name if codec_name else "N/A"
                
                f.write(f"# {size_str} | {bitrate_str} | {duration_str} | {resolution} | {codec_str}\n")
                f.write(f"{file_path}\n\n")
        
        if not short_format:
            # Summary statistics for full format
            f.write("#" + "="*100 + "\n")
            f.write(f"# SUMMARY:\n")
            f.write(f"# Files: {len(results)}\n")
            f.write(f"# Total size: {format_file_size(total_size)}\n")
            f.write(f"# Total duration: {format_duration(total_duration)}\n")
    
    conn.close()
    
    # Output statistics to screen
    print(f"\n{Fore.GREEN}✅ File list exported to: {output_file}{Style.RESET_ALL}")
    print(f"Files found: {len(results)}")
    print(f"Criteria: bitrate ≥{min_bitrate_mbps} Mbit/s, size ≥{min_size_mb} MB")
    print(f"Format: {'Short (paths only)' if short_format else 'Full (with metadata)'}")
    print(f"Total size: {format_file_size(sum(row[2] for row in results if row[2]))}")
    
    # Show examples
    print(f"\n{Fore.CYAN}Examples of found files:{Style.RESET_ALL}")
    for i, row in enumerate(results[:5]):
        file_path, file_name, file_size, bit_rate, duration, resolution, codec_name = row
        size_str = format_file_size(file_size)
        bitrate_str = format_bitrate(bit_rate)
        print(f"  {i+1}. {file_name} ({size_str}, {bitrate_str})")
    
    if len(results) > 5:
        print(f"  ... and {len(results) - 5} more files")

def export_duplicates_list(db_path, output_file, path_pattern=None, short_format=False):
    """Exports duplicate list to text file"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Search by hash (exact duplicates)
    query = '''
        SELECT file_hash, COUNT(*) as cnt
        FROM videos 
        WHERE file_hash IS NOT NULL AND file_hash != '' AND is_corrupted = 0
        GROUP BY file_hash
        HAVING COUNT(*) >= 2
        ORDER BY cnt DESC
    '''
    cursor.execute(query)
    groups = cursor.fetchall()
    method = "hash"
    
    if not groups:
        print(f"{Fore.YELLOW}Duplicates by {method} not found{Style.RESET_ALL}")
        conn.close()
        return
    
    with open(output_file, 'w', encoding='utf-8') as f:
        # Header
        if not short_format:
            f.write(f"# Duplicate list by {method}\n")
            f.write(f"# Found {len(groups)} duplicate groups\n")
            if path_pattern:
                f.write(f"# Filtered by pattern: {path_pattern}\n")
            f.write(f"# Created: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("#\n")
            f.write("#" + "="*100 + "\n\n")
        
        total_files = 0
        total_wasted_space = 0
        
        for i, (key_value, count) in enumerate(groups, 1):
            # Get files in group
            detail_query = '''
                SELECT file_path, file_name, file_size, duration, bit_rate,
                       width || 'x' || height as resolution, codec_name
                FROM videos 
                WHERE file_hash = ? AND is_corrupted = 0
                ORDER BY file_size DESC
            '''
            
            cursor.execute(detail_query, (key_value,))
            files = cursor.fetchall()
            
            if not files:
                continue
                
            group_size = files[0][2] if files else 0
            wasted = group_size * (count - 1)
            total_wasted_space += wasted
            
            # Filter by pattern if specified
            filtered_files = []
            for file_data in files:
                file_path = file_data[0]
                if path_pattern is None or path_pattern in file_path:
                    filtered_files.append(file_data)
            
            if not filtered_files:
                continue
            
            if short_format:
                # Export only file names
                for file_path, file_name, file_size, duration, bit_rate, resolution, codec_name in filtered_files:
                    f.write(f"{file_path}\n")
                    total_files += 1
            else:
                # Export full information
                f.write(f"# Group {i}: {len(files)} copies total, {len(filtered_files)} matching pattern, hash: {key_value[:16]}...\n")
                f.write(f"# Total size: {format_file_size(group_size * len(files))}, wasted: {format_file_size(wasted)}\n")
                f.write("#\n")
                
                # Show all files in group first (for context)
                f.write("# All files in group:\n")
                for j, (file_path, file_name, file_size, duration, bit_rate, resolution, codec_name) in enumerate(files):
                    size_str = format_file_size(file_size)
                    duration_str = format_duration(duration)
                    bitrate_str = format_bitrate(bit_rate)
                    codec_str = codec_name[:8] if codec_name else "N/A"
                    
                    status = "ORIGINAL" if j == 0 else f"COPY {j}"
                    is_matching = path_pattern is None or path_pattern in file_path
                    marker = " ← MATCHING PATTERN" if is_matching else ""
                    f.write(f"# {status}: {size_str} | {duration_str} | {bitrate_str} | {resolution} | {codec_str}{marker}\n")
                    f.write(f"# {file_path}\n")
                
                f.write("#\n# Files to process (matching pattern):\n")
                
                # Then export only filtered files for processing
                for file_path, file_name, file_size, duration, bit_rate, resolution, codec_name in filtered_files:
                    f.write(f"{file_path}\n")
                    total_files += 1
                
                f.write("#\n")
            
    conn.close()
    
    print(f"\n{Fore.GREEN}✅ Duplicate list exported to: {output_file}{Style.RESET_ALL}")
    print(f"Duplicate groups found: {len(groups)}")
    if path_pattern:
        print(f"Filtered by pattern: '{path_pattern}'")
    print(f"Total files exported: {total_files}")
    print(f"Format: {'Short (paths only)' if short_format else 'Full (with metadata)'}")
    print(f"Space that can be freed: {Fore.RED}{format_file_size(total_wasted_space)}{Style.RESET_ALL}")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Video file database queries'
    )
    parser.add_argument(
        '--database', '-d',
        default='video_analysis.db',
        help='Path to SQLite database (default: video_analysis.db)'
    )
    parser.add_argument(
        '--largest', '-l',
        type=int,
        default=0,
        metavar='N',
        help='Show N largest files'
    )
    parser.add_argument(
        '--high-bitrate', '-b',
        type=int,
        default=0,
        metavar='N',
        help='Show N files with high bitrate (≥10 Mbit/s)'
    )
    parser.add_argument(
        '--longest', '-t',
        type=int,
        default=0,
        metavar='N',
        help='Show N longest files'
    )
    parser.add_argument(
        '--min-bitrate',
        type=int,
        default=10,
        help='Minimum bitrate in Mbit/s for filter (default: 10)'
    )
    parser.add_argument(
        '--all', '-a',
        action='store_true',
        help='Show all types of reports (top-20 for each)'
    )
    parser.add_argument(
        '--export-list', '-e',
        metavar='FILE',
        help='Export file list to text file by given criteria'
    )
    parser.add_argument(
        '--export-min-bitrate',
        type=int,
        default=15,
        metavar='MBPS',
        help='Minimum bitrate for export in Mbit/s (default: 15)'
    )
    parser.add_argument(
        '--export-min-size',
        type=int,
        default=50,
        metavar='MB',
        help='Minimum file size for export in MB (default: 50)'
    )
    parser.add_argument(
        '--min-duplicates',
        type=int,
        default=2,
        metavar='N',
        help='Minimum number of duplicates in group (default: 2)'
    )
    parser.add_argument(
        '--export-duplicates',
        metavar='FILE',
        help='Export duplicate list to text file'
    )
    parser.add_argument(
        '--export-pattern',
        metavar='PATTERN',
        help='Filter duplicates by path pattern (e.g., "Camera Uploads")'
    )
    parser.add_argument(
        '--short', '-s',
        action='store_true',
        help='Export only file names (short format)'
    )
    
    args = parser.parse_args()
    
    # Check database existence
    try:
        conn = sqlite3.connect(args.database)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM videos")
        total_files = cursor.fetchone()[0]
        conn.close()
        
        if total_files == 0:
            print(f"{Fore.YELLOW}Database is empty. First run video file analysis.{Style.RESET_ALL}")
            return
            
        print(f"{Fore.GREEN}Database contains {total_files} records{Style.RESET_ALL}")
        
    except sqlite3.Error as e:
        print(f"{Fore.RED}Database access error: {e}{Style.RESET_ALL}")
        return
    
    # Export file list
    if args.export_list:
        export_files_list(args.database, args.export_list, args.export_min_bitrate, args.export_min_size, args.short)
        return
    
    # Export duplicates
    if args.export_duplicates:
        export_duplicates_list(args.database, args.export_duplicates, args.export_pattern, args.short)
        return
    
    # Execute queries
    if args.all:
        query_largest_files(args.database, 20)
        query_high_bitrate_files(args.database, args.min_bitrate, 20)
        query_longest_files(args.database, 20)
    else:
        if args.largest:
            query_largest_files(args.database, args.largest)
        
        if args.high_bitrate:
            query_high_bitrate_files(args.database, args.min_bitrate, args.high_bitrate)
        
        if args.longest:
            query_longest_files(args.database, args.longest)
        
        # If nothing selected, show top-10 largest
        if not any([args.largest, args.high_bitrate, args.longest]):
            query_largest_files(args.database, 10)

if __name__ == "__main__":
    main()
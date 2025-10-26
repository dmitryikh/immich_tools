#!/usr/bin/env python3
"""
Media Database Query Tool

Utility for querying media file database (videos and images) with convenient formatting.

Usage:
python media_query.py --export-duplicates duplicates_by_hash.txt --export-pattern 'Camera Uploads' --duplicate-patterns 'Camera Uploads' 'copy' '_copy'

python media_query.py --export-list high_quality_files.txt --export-min-bitrate 15 --export-min-size 50

python media_query.py --export-no-metadata files_without_creation_date.txt

python media_query.py --export-dirs directory_structure.txt
"""

import sqlite3
import argparse
import datetime
import os
import sys
from colorama import Fore, Style, init
from collections import defaultdict

# Import from local library
from lib.utils import sort_files_by_directory_depth, RAW_EXTENSIONS, StripAnsiWriter
from lib.video_converter import OUTDATED_CODECS, OUTDATED_FORMATS
from lib.db import query_all_database

# Initialize colorama with forced colors for container support
init(autoreset=True, strip=False)

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

def write_export_file(output_file, file_list, export_type, short_format=False, current_time=None, **kwargs):
    """
    Unified function to write export files with consistent formatting
    
    Args:
        output_file: Output file path
        file_list: List of file records
        export_type: Type of export for header (e.g., "high bitrate files", "RAW files")
        short_format: Whether to use short format (paths only)
        current_time: datetime object for deterministic output (default: now)
        **kwargs: Additional parameters for specific export types
    """
    if current_time is None:
        current_time = datetime.datetime.now()
        
    with open(output_file, 'w', encoding='utf-8') as f:
        total_size = 0
        video_count = 0
        image_count = 0
        
        if not short_format:
            # Header for full format
            f.write(f"# List of {export_type}\n")
            f.write(f"# Found {len(file_list)} files\n")
            f.write(f"# Created: {current_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            # Add specific criteria info
            if 'min_bitrate' in kwargs:
                f.write(f"# Criteria: bitrate ≥{kwargs['min_bitrate']} Mbit/s, size ≥{kwargs.get('min_size', 50)} MB\n")
            elif 'suffix' in kwargs:
                f.write(f"# Criteria: files with suffix '{kwargs['suffix']}' that have corresponding originals\n")
            
            f.write("#\n")
            if kwargs.get('include_potential_dates'):
                f.write("# Format: file_path | type | size | duration | bitrate | resolution | codec | mtime\n")
            else:
                f.write("# Format: file_path | type | size | duration | bitrate | resolution | codec\n")
            f.write("#" + "="*100 + "\n\n")
        
        for row in file_list:
            # Handle different record formats
            if len(row) >= 10 and kwargs.get('include_potential_dates'):
                # Enhanced format with potential creation dates
                file_path, file_name, file_size, media_type, duration, bit_rate, resolution, codec_name, potential_date, date_source = row[:10]
            elif len(row) >= 8:  # Full record format
                file_path, file_name, file_size, media_type, duration, bit_rate, resolution, codec_name = row[:8]
                potential_date = date_source = None
            elif len(row) >= 7:  # Video record format (no media_type)
                file_path, file_name, file_size, bit_rate, duration, resolution, codec_name = row
                media_type = 'video'  # Assume video for bitrate queries
                potential_date = date_source = None
            else:
                # Minimal format - extract what we can
                file_path = row[0]
                file_name = os.path.basename(file_path)
                file_size = row[2] if len(row) > 2 else None
                media_type = 'unknown'
                duration = bit_rate = resolution = codec_name = None
                potential_date = date_source = None
            
            total_size += file_size if file_size else 0
            if media_type == 'video':
                video_count += 1
            elif media_type == 'image':
                image_count += 1
            
            if short_format:
                # Short format: only file paths
                f.write(f"{file_path}\n")
            else:
                # Full format: file path with metadata
                size_str = format_file_size(file_size)
                duration_str = format_duration(duration) if duration else "N/A"
                bitrate_str = format_bitrate(bit_rate) if bit_rate else "N/A"
                codec_str = codec_name if codec_name else "N/A"
                resolution_str = resolution if resolution else "N/A"
                
                # For no-metadata files, add mtime info
                if kwargs.get('include_potential_dates'):
                    # Get mtime for the file
                    mtime_str = "N/A"
                    try:
                        if os.path.exists(file_path):
                            import time
                            mtime = os.path.getmtime(file_path)
                            mtime_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mtime))
                    except (OSError, ValueError):
                        mtime_str = "N/A"
                    
                    f.write(f"# {media_type.upper()} | {size_str} | {duration_str} | {bitrate_str} | {resolution_str} | {codec_str} | {mtime_str}\n")
                    f.write(f"{file_path}\n")
                    
                    # Add potential creation time suggestion if available
                    if potential_date and date_source == "from path":
                        f.write(f"# From path:\n")
                        f.write(f"CREATION_TIME {potential_date}\n")
                    
                    f.write("\n")
                else:
                    f.write(f"# {media_type.upper()} | {size_str} | {duration_str} | {bitrate_str} | {resolution_str} | {codec_str}\n")
                    f.write(f"{file_path}\n\n")
        
        if not short_format:
            # Summary statistics for full format
            f.write("#" + "="*100 + "\n")
            f.write(f"# SUMMARY:\n")
            f.write(f"# Total files: {len(file_list)}")
            if video_count > 0 or image_count > 0:
                f.write(f" (Videos: {video_count}, Images: {image_count})")
            f.write(f"\n# Total size: {format_file_size(total_size)}\n")
            
            # Add total duration for videos
            total_duration = sum(row[4] if len(row) > 4 and row[4] else 0 for row in file_list)
            if total_duration > 0:
                f.write(f"# Total duration: {format_duration(total_duration)}\n")

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
        FROM media_files 
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
        resolution_str = resolution if resolution else "N/A"
        
        # Color highlighting
        status_color = Fore.RED if is_corrupted else Fore.GREEN
        size_color = Fore.MAGENTA if file_size and file_size > 1_000_000_000 else Fore.BLUE  # > 1GB
        
        print(f"{i:<3} {size_color}{size_str:<10}{Style.RESET_ALL} {duration_str:<8} {bitrate_str:<12} "
              f"{resolution_str:<10} {codec_str:<8} {status_color}{status_str:<6}{Style.RESET_ALL} {file_name}")
    
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
        FROM media_files 
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
        
        resolution_str = resolution if resolution else "N/A"
        
        print(f"{i:<3} {bitrate_color}{bitrate_str:<12}{Style.RESET_ALL} {size_str:<10} {duration_str:<8} "
              f"{resolution_str:<10} {codec_str:<8} {file_name}")
    
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
        FROM media_files 
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
        resolution_str = resolution if resolution else "N/A"
        
        print(f"{i:<3} {duration_color}{duration_str:<10}{Style.RESET_ALL} {size_str:<10} {bitrate_str:<12} "
              f"{resolution_str:<10} {codec_str:<8} {file_name}")
    
    conn.close()

def export_raw_files(db_path, output_file, short_format=False, current_time=None):
    """Exports RAW image files to text file"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Build query to find RAW files using file extensions
    raw_extensions_tuple = tuple(RAW_EXTENSIONS)
    placeholders = ', '.join('?' * len(raw_extensions_tuple))
    
    query = f'''
        SELECT 
            file_path,
            file_name,
            file_size,
            media_type,
            duration,
            bit_rate,
            width || 'x' || height as resolution,
            codec_name
        FROM media_files 
        WHERE is_corrupted = 0 
          AND media_type = 'image'
          AND LOWER(SUBSTR(file_path, -4)) IN ({placeholders})
        ORDER BY file_path
    '''
    
    cursor.execute(query, [ext.lower() for ext in raw_extensions_tuple])
    results = cursor.fetchall()
    
    if not results:
        print(f"{Fore.YELLOW}No RAW files found{Style.RESET_ALL}")
        conn.close()
        return
    
    # Sort files by directory structure (subdirectories first, then lexicographically)
    results = sort_files_by_directory_depth(results)
    
    # Use unified export function
    write_export_file(output_file, results, "RAW image files", short_format, current_time)
    
    conn.close()
    
    # Output statistics to screen
    print(f"\n{Fore.GREEN}✅ RAW files list exported to: {output_file}{Style.RESET_ALL}")
    print(f"RAW files found: {len(results)}")
    print(f"Format: {'Short (paths only)' if short_format else 'Full (with metadata)'}")
    print(f"Total size: {format_file_size(sum(row[2] for row in results if row[2]))}")
    
    # Show examples by extension
    print(f"\n{Fore.CYAN}Examples of RAW files found:{Style.RESET_ALL}")
    
    # Group by extension for display
    extensions_found = {}
    for row in results:
        file_path = row[0]
        ext = os.path.splitext(file_path)[1].lower()
        if ext not in extensions_found:
            extensions_found[ext] = []
        extensions_found[ext].append(row)
    
    # Show examples for each extension
    for ext, files in sorted(extensions_found.items()):
        print(f"  {Fore.BLUE}{ext.upper()} files:{Style.RESET_ALL} {len(files)} found")
        for i, row in enumerate(files[:2]):
            file_path, file_name, file_size, media_type, duration, bit_rate, resolution, codec_name = row
            size_str = format_file_size(file_size)
            print(f"    {i+1}. {file_name} ({size_str}, {resolution})")
    
    if len(results) > sum(len(files[:2]) for files in extensions_found.values()):
        print(f"  ... and more files")

def export_old_video_files(db_path, output_file, short_format=False, current_time=None):
    """Exports video files with outdated codecs or formats to text file"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Build query to find videos with outdated codecs or formats
    # We need to check both codec_name and format_name fields
    outdated_codecs_tuple = tuple(OUTDATED_CODECS)
    outdated_formats_tuple = tuple(OUTDATED_FORMATS)
    
    codecs_placeholders = ', '.join('?' * len(outdated_codecs_tuple))
    formats_placeholders = ', '.join('?' * len(outdated_formats_tuple))
    
    query = f'''
        SELECT 
            file_path,
            file_name,
            file_size,
            media_type,
            duration,
            bit_rate,
            width || 'x' || height as resolution,
            codec_name,
            format_name
        FROM media_files 
        WHERE is_corrupted = 0 
          AND media_type = 'video'
          AND (
            codec_name IN ({codecs_placeholders})
            OR format_name IN ({formats_placeholders})
          )
        ORDER BY file_size DESC
    '''
    
    # Combine parameters for both codec and format checks
    query_params = list(outdated_codecs_tuple) + list(outdated_formats_tuple)
    
    cursor.execute(query, query_params)
    results = cursor.fetchall()
    
    if not results:
        print(f"{Fore.YELLOW}No video files with outdated codecs/formats found{Style.RESET_ALL}")
        conn.close()
        return
    
    # Sort files by directory structure (subdirectories first, then lexicographically)
    results = sort_files_by_directory_depth(results)
    
    # Use unified export function (need to adjust for format_name field)
    # Convert results to match expected format for write_export_file
    converted_results = []
    for row in results:
        # Extract format_name and include it in the display
        file_path, file_name, file_size, media_type, duration, bit_rate, resolution, codec_name, format_name = row
        
        # Create a modified codec field that includes format info
        codec_with_format = f"{codec_name or 'N/A'}"
        if format_name and format_name in OUTDATED_FORMATS:
            codec_with_format += f" (format: {format_name})"
        
        # Convert back to 8-field format expected by write_export_file
        converted_row = (file_path, file_name, file_size, media_type, duration, bit_rate, resolution, codec_with_format)
        converted_results.append(converted_row)
    
    write_export_file(output_file, converted_results, "video files with outdated codecs/formats", short_format, current_time)
    
    conn.close()
    
    # Output statistics to screen
    print(f"\n{Fore.GREEN}✅ Old video files list exported to: {output_file}{Style.RESET_ALL}")
    print(f"Video files with outdated codecs/formats: {len(results)}")
    print(f"Format: {'Short (paths only)' if short_format else 'Full (with metadata)'}")
    print(f"Total size: {format_file_size(sum(row[2] for row in results if row[2]))}")
    
    # Show examples by codec/format type
    print(f"\n{Fore.CYAN}Examples of old video files found:{Style.RESET_ALL}")
    
    # Group by codec and format for display
    codecs_found = {}
    formats_found = {}
    
    for row in results:
        file_path, file_name, file_size, media_type, duration, bit_rate, resolution, codec_name, format_name = row
        
        # Group by codec
        if codec_name and codec_name in OUTDATED_CODECS:
            if codec_name not in codecs_found:
                codecs_found[codec_name] = []
            codecs_found[codec_name].append(row)
        
        # Group by format 
        if format_name and format_name in OUTDATED_FORMATS:
            if format_name not in formats_found:
                formats_found[format_name] = []
            formats_found[format_name].append(row)
    
    # Show examples for each outdated codec
    if codecs_found:
        print(f"  {Fore.RED}Outdated Codecs:{Style.RESET_ALL}")
        for codec, files in sorted(codecs_found.items()):
            print(f"    {Fore.BLUE}{codec}:{Style.RESET_ALL} {len(files)} found")
            for i, row in enumerate(files[:2]):
                file_path, file_name, file_size, media_type, duration, bit_rate, resolution, codec_name, format_name = row
                size_str = format_file_size(file_size)
                duration_str = format_duration(duration)
                print(f"      {i+1}. {file_name} ({size_str}, {duration_str}, {resolution})")
    
    # Show examples for each outdated format
    if formats_found:
        print(f"  {Fore.MAGENTA}Outdated Formats:{Style.RESET_ALL}")
        for format_name, files in sorted(formats_found.items()):
            print(f"    {Fore.BLUE}{format_name}:{Style.RESET_ALL} {len(files)} found")
            for i, row in enumerate(files[:2]):
                file_path, file_name, file_size, media_type, duration, bit_rate, resolution, codec_name, format_name = row
                size_str = format_file_size(file_size)
                duration_str = format_duration(duration)
                codec_str = codec_name if codec_name else "N/A"
                print(f"      {i+1}. {file_name} ({size_str}, {duration_str}, {resolution}, codec: {codec_str})")
    
    # Show total counts
    total_codec_files = sum(len(files) for files in codecs_found.values())
    total_format_files = sum(len(files) for files in formats_found.values())
    
    if total_codec_files > 0 or total_format_files > 0:
        print(f"  Summary: {total_codec_files} files with outdated codecs, {total_format_files} files with outdated formats")
        # Note: some files might have both outdated codec AND format, so total may be less than sum
        if total_codec_files + total_format_files > len(results):
            print(f"  (Some files have both outdated codec and format)")

def export_corrupted_files(db_path, output_file, short_format=False, current_time=None):
    """Exports corrupted files (is_corrupted = 1) to text file"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = '''
        SELECT 
            file_path,
            file_name,
            file_size,
            media_type,
            duration,
            bit_rate,
            width || 'x' || height as resolution,
            codec_name
        FROM media_files 
        WHERE is_corrupted = 1
        ORDER BY file_path
    '''
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    if not results:
        print(f"{Fore.YELLOW}No corrupted files found{Style.RESET_ALL}")
        conn.close()
        return
    
    # Sort files by directory structure (subdirectories first, then lexicographically)
    results = sort_files_by_directory_depth(results)
    
    # Use unified export function
    write_export_file(output_file, results, "corrupted files", short_format, current_time)
    
    conn.close()
    
    # Output statistics to screen
    print(f"\n{Fore.GREEN}✅ Corrupted files list exported to: {output_file}{Style.RESET_ALL}")
    print(f"Corrupted files found: {len(results)}")
    print(f"Format: {'Short (paths only)' if short_format else 'Full (with metadata)'}")
    print(f"Total size: {format_file_size(sum(row[2] for row in results if row[2]))}")
    
    # Show examples by media type
    print(f"\n{Fore.CYAN}Examples of corrupted files found:{Style.RESET_ALL}")
    
    # Group by media type for display
    media_types = {}
    for row in results:
        file_path, file_name, file_size, media_type, duration, bit_rate, resolution, codec_name = row
        if media_type not in media_types:
            media_types[media_type] = []
        media_types[media_type].append(row)
    
    # Show examples for each media type
    for media_type, files in sorted(media_types.items()):
        print(f"  {Fore.BLUE}{media_type.upper()} files:{Style.RESET_ALL} {len(files)} found")
        for i, row in enumerate(files[:2]):
            file_path, file_name, file_size, media_type, duration, bit_rate, resolution, codec_name = row
            size_str = format_file_size(file_size)
            if media_type == 'video':
                duration_str = format_duration(duration)
                print(f"    {i+1}. {file_name} ({size_str}, {duration_str}, {resolution})")
            else:
                print(f"    {i+1}. {file_name} ({size_str}, {resolution})")
    
    if len(results) > sum(len(files[:2]) for files in media_types.values()):
        print(f"  ... and more files")

def export_files_list(db_path, output_file, min_bitrate_mbps=15, min_size_mb=50, short_format=False, current_time=None):
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
        FROM media_files 
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
    
    # Sort files by directory structure (subdirectories first, then lexicographically)
    results = sort_files_by_directory_depth(results)
    
    # Use unified export function
    write_export_file(output_file, results, f"video files with bitrate ≥{min_bitrate_mbps} Mbit/s and size ≥{min_size_mb} MB", 
                      short_format, current_time, min_bitrate=min_bitrate_mbps, min_size=min_size_mb)
    
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

def export_files_with_suffix(db_path, output_file, suffix, short_format=False, current_time=None):
    """Exports files with given suffix that have corresponding original files without suffix in same directory"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all files from database
    query = '''
        SELECT 
            file_path,
            file_name,
            file_size,
            media_type,
            duration,
            bit_rate,
            width || 'x' || height as resolution,
            codec_name
        FROM media_files 
        WHERE is_corrupted = 0
        ORDER BY file_path
    '''
    
    cursor.execute(query)
    all_files = cursor.fetchall()
    
    # Build directory index for fast lookup
    # Structure: {directory: {base_name_without_ext: [file_records]}}
    dir_index = {}
    
    for file_record in all_files:
        file_path = file_record[0]
        dir_name = os.path.dirname(file_path)
        file_name = os.path.basename(file_path)
        name_without_ext = os.path.splitext(file_name)[0]
        
        if dir_name not in dir_index:
            dir_index[dir_name] = {}
        
        if name_without_ext not in dir_index[dir_name]:
            dir_index[dir_name][name_without_ext] = []
        
        dir_index[dir_name][name_without_ext].append(file_record)
    
    # Find files with suffix that have corresponding originals
    suffix_files = []
    
    for dir_name, files_by_base in dir_index.items():
        for base_name, file_records in files_by_base.items():
            # Check if this base name ends with the suffix
            if base_name.endswith(suffix):
                # Get base name without suffix
                original_base = base_name[:-len(suffix)]
                
                # Check if original exists in same directory
                if original_base in files_by_base:
                    # Found files with suffix that have corresponding originals
                    for file_record in file_records:
                        suffix_files.append((file_record, original_base))
    
    if not suffix_files:
        print(f"{Fore.YELLOW}No files with suffix '{suffix}' found that have corresponding originals{Style.RESET_ALL}")
        conn.close()
        return
    
    # Sort files using common sorting function
    suffix_files = sort_files_by_directory_depth(suffix_files)
    
    # Write to file
    if current_time is None:
        current_time = datetime.datetime.now()
        
    with open(output_file, 'w', encoding='utf-8') as f:
        total_size = 0
        video_count = 0
        image_count = 0
        
        if not short_format:
            # Header for full format
            f.write(f"# List of files with suffix '{suffix}' that have corresponding originals\n")
            f.write(f"# Found {len(suffix_files)} files\n")
            f.write(f"# Created: {current_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("#\n")
            f.write("# Format: file_path | type | size | duration | bitrate | resolution | codec | original_base\n")
            f.write("#" + "="*100 + "\n\n")
        
        for file_record, original_base in suffix_files:
            file_path, file_name, file_size, media_type, duration, bit_rate, resolution, codec_name = file_record
            
            total_size += file_size if file_size else 0
            if media_type == 'video':
                video_count += 1
            else:
                image_count += 1
            
            if short_format:
                # Short format: only file paths
                f.write(f"{file_path}\n")
            else:
                # Full format: file path with metadata and original info
                size_str = format_file_size(file_size)
                duration_str = format_duration(duration) if duration else "N/A"
                bitrate_str = format_bitrate(bit_rate)
                codec_str = codec_name if codec_name else "N/A"
                
                f.write(f"# {media_type.upper()} | {size_str} | {duration_str} | {bitrate_str} | {resolution} | {codec_str} | original: {original_base}\n")
                f.write(f"{file_path}\n\n")
        
        if not short_format:
            # Summary statistics for full format
            f.write("#" + "="*100 + "\n")
            f.write(f"# SUMMARY:\n")
            f.write(f"# Total files with suffix '{suffix}': {len(suffix_files)} (Videos: {video_count}, Images: {image_count})\n")
            f.write(f"# Total size: {format_file_size(total_size)}\n")
    
    conn.close()
    
    # Output statistics to screen
    print(f"\n{Fore.GREEN}✅ Files with suffix '{suffix}' exported to: {output_file}{Style.RESET_ALL}")
    print(f"Files with suffix that have originals: {len(suffix_files)} (Videos: {video_count}, Images: {image_count})")
    print(f"Format: {'Short (paths only)' if short_format else 'Full (with metadata)'}")
    print(f"Total size: {format_file_size(total_size)}")
    
    # Show examples
    print(f"\n{Fore.CYAN}Examples of files with suffix '{suffix}':{Style.RESET_ALL}")
    
    for i, (file_record, original_base) in enumerate(suffix_files[:5]):
        file_path, file_name, file_size, media_type, duration, bit_rate, resolution, codec_name = file_record
        size_str = format_file_size(file_size)
        dir_name = os.path.dirname(file_path)
        
        print(f"  {i+1}. {file_name} ({size_str}) -> original: {original_base}.*")
        print(f"      Directory: {dir_name}")
    
    if len(suffix_files) > 5:
        print(f"  ... and {len(suffix_files) - 5} more files")

def export_no_metadata_files(db_path, output_file, short_format=False, current_time=None):
    """Exports files without creation_date metadata to text file"""
    import os
    from datetime import datetime
    from lib.utils import parse_datetime_from_path
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = '''
        SELECT 
            file_path,
            file_name,
            file_size,
            media_type,
            duration,
            bit_rate,
            width || 'x' || height as resolution,
            codec_name
        FROM media_files 
        WHERE creation_date IS NULL AND is_corrupted = 0
        ORDER BY media_type, file_size DESC
    '''
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    if not results:
        print(f"{Fore.YELLOW}All files have creation_date metadata{Style.RESET_ALL}")
        conn.close()
        return
    
    # Sort files by directory structure (subdirectories first, then lexicographically)
    results = sort_files_by_directory_depth(results)
    
    # Enhance results with potential creation time information
    enhanced_results = []
    for row in results:
        file_path = row[0]
        potential_creation_time = None
        creation_source = None
        
        # Try parsing from path
        parsed_date = parse_datetime_from_path(file_path)
        if parsed_date:
            potential_creation_time = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
            creation_source = "from path"
        else:
            # Try mtime as fallback
            try:
                if os.path.exists(file_path):
                    mtime = os.path.getmtime(file_path)
                    mtime_date = datetime.fromtimestamp(mtime)
                    potential_creation_time = mtime_date.strftime('%Y-%m-%d %H:%M:%S')
                    creation_source = "from mtime"
            except (OSError, ValueError):
                potential_creation_time = None
                creation_source = None
        
        # Add potential creation info to the row
        enhanced_row = row + (potential_creation_time, creation_source)
        enhanced_results.append(enhanced_row)
    
    # Use unified export function with enhanced data
    write_export_file(output_file, enhanced_results, "files without creation_date metadata", 
                     short_format, current_time, include_potential_dates=True)
    
    conn.close()
    
    # Output statistics to screen with potential creation time info
    image_count = len([row for row in enhanced_results if row[3] == 'image'])
    video_count = len([row for row in enhanced_results if row[3] == 'video'])
    
    print(f"\n{Fore.GREEN}✅ No-metadata files list exported to: {output_file}{Style.RESET_ALL}")
    print(f"Files without creation_date: {len(enhanced_results)} (Images: {image_count}, Videos: {video_count})")
    print(f"Format: {'Short (paths only)' if short_format else 'Full (with metadata)'}")
    print(f"Total size: {format_file_size(sum(row[2] for row in enhanced_results if row[2]))}")
    
    # Show examples by type with potential creation time
    print(f"\n{Fore.CYAN}Examples of files without metadata:{Style.RESET_ALL}")
    
    # Show images first
    image_examples = [row for row in enhanced_results if row[3] == 'image'][:3]
    if image_examples:
        print(f"  {Fore.BLUE}Images:{Style.RESET_ALL}")
        for i, row in enumerate(image_examples):
            file_path, file_name, file_size, media_type, duration, bit_rate, resolution, codec_name = row[:8]
            potential_date = row[8] if len(row) > 8 else None
            date_source = row[9] if len(row) > 9 else None
            
            size_str = format_file_size(file_size)
            
            # Show potential creation time from enhanced data
            creation_info = ""
            if potential_date and date_source:
                creation_info = f", potential date: {potential_date} [{date_source}]"
            else:
                creation_info = ", no potential date found"
            
            print(f"    {i+1}. {file_name} ({size_str}, {resolution}{creation_info})")
    
    # Show videos
    video_examples = [row for row in enhanced_results if row[3] == 'video'][:3]
    if video_examples:
        print(f"  {Fore.MAGENTA}Videos:{Style.RESET_ALL}")
        for i, row in enumerate(video_examples):
            file_path, file_name, file_size, media_type, duration, bit_rate, resolution, codec_name = row[:8]
            potential_date = row[8] if len(row) > 8 else None
            date_source = row[9] if len(row) > 9 else None
            
            size_str = format_file_size(file_size)
            duration_str = format_duration(duration)
            codec_str = codec_name if codec_name else "N/A"
            
            # Show potential creation time from enhanced data
            creation_info = ""
            if potential_date and date_source:
                creation_info = f", potential date: {potential_date} [{date_source}]"
            else:
                creation_info = ", no potential date found"
            
            print(f"    {i+1}. {file_name} ({size_str}, {duration_str}, {codec_str}{creation_info})")
    
    remaining = len(enhanced_results) - len(image_examples) - len(video_examples)
    if remaining > 0:
        print(f"  ... and {remaining} more files")

def determine_original_and_copies(files, duplicate_patterns=None):
    """
    Determines which file is the original and which are copies based on the algorithm:
    1. Sort files lexicographically by full path
    2. While more than one file remains as potential ORIGINAL:
       2.1. If file contains duplicate patterns, mark as COPY
       2.2. If all files are processed and still >1 potential ORIGINAL, 
            keep the one with smallest lexicographic order as ORIGINAL
    
    Returns: (original_file, copy_files)
    """
    if not files:
        return None, []
    
    # Sort files lexicographically by full path (index 0 is file_path)
    sorted_files = sorted(files, key=lambda x: x[0])
    
    if len(sorted_files) == 1:
        return sorted_files[0], []
    
    # Mark files as potential originals or copies
    potential_originals = []
    copies = []
    
    for file_data in sorted_files:
        file_path = file_data[0]
        is_copy = False
        
        # Check if file matches any duplicate pattern
        if duplicate_patterns:
            for pattern in duplicate_patterns:
                if pattern in file_path:
                    is_copy = True
                    break
        
        if is_copy:
            copies.append(file_data)
        else:
            potential_originals.append(file_data)
    
    # If no potential originals left (all matched patterns), 
    # take the first (lexicographically smallest) as original
    if not potential_originals:
        original = sorted_files[0]
        copies = sorted_files[1:]
    # If only one potential original, use it
    elif len(potential_originals) == 1:
        original = potential_originals[0]
        # Add remaining files to copies if not already there
        for file_data in sorted_files:
            if file_data != original and file_data not in copies:
                copies.append(file_data)
    # If multiple potential originals, use lexicographically first
    else:
        original = potential_originals[0]
        copies = potential_originals[1:] + copies
    
    return original, copies

def export_duplicates_list(db_path, output_file, path_pattern=None, short_format=False, duplicate_patterns=None, current_time=None):
    """Exports duplicate list to text file"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Search by hash (exact duplicates)
    query = '''
        SELECT file_hash, COUNT(*) as cnt
        FROM media_files 
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
    
    if current_time is None:
        current_time = datetime.datetime.now()
    
    with open(output_file, 'w', encoding='utf-8') as f:
        # Header
        if not short_format:
            f.write(f"# Duplicate list by {method}\n")
            f.write(f"# Found {len(groups)} duplicate groups\n")
            if path_pattern:
                f.write(f"# Filtered by pattern: {path_pattern}\n")
            f.write(f"# Created: {current_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("#\n")
            f.write("#" + "="*100 + "\n\n")
        
        total_files = 0
        total_wasted_space = 0
        
        for i, (key_value, count) in enumerate(groups, 1):
            # Get files in group
            detail_query = '''
                SELECT file_path, file_name, file_size, duration, bit_rate,
                       width || 'x' || height as resolution, codec_name
                FROM media_files 
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
            
            # Determine original and copies using new algorithm
            original_file, copy_files = determine_original_and_copies(files, duplicate_patterns)
            
            # Filter by pattern if specified (apply to copies only, keep original for context)
            filtered_copies = []
            for file_data in copy_files:
                file_path = file_data[0]
                if path_pattern is None or path_pattern in file_path:
                    filtered_copies.append(file_data)
            
            # Skip group if no copies match the pattern
            if not filtered_copies:
                continue
            
            if short_format:
                # Export only copy file paths (not original)
                for file_path, file_name, file_size, duration, bit_rate, resolution, codec_name in filtered_copies:
                    f.write(f"{file_path}\n")
                    total_files += 1
            else:
                # Export full information with original/copy classification
                f.write(f"# Group {i}: {len(files)} files total, {len(filtered_copies)} copies to process, hash: {key_value[:16]}...\n")
                f.write(f"# Total size: {format_file_size(group_size * len(files))}, wasted: {format_file_size(wasted)}\n")
                f.write("#\n")
                
                # Show all files in group with classification
                f.write("# File classification:\n")
                
                # Show original first
                if original_file:
                    file_path, file_name, file_size, duration, bit_rate, resolution, codec_name = original_file
                    size_str = format_file_size(file_size)
                    duration_str = format_duration(duration)
                    bitrate_str = format_bitrate(bit_rate)
                    codec_str = codec_name[:8] if codec_name else "N/A"
                    
                    is_matching = path_pattern is None or path_pattern in file_path
                    marker = " ← MATCHES PATTERN" if is_matching else ""
                    f.write(f"# ORIGINAL: {size_str} | {duration_str} | {bitrate_str} | {resolution} | {codec_str}{marker}\n")
                    f.write(f"# {file_path}\n")
                
                # Show copies
                for j, file_data in enumerate(copy_files, 1):
                    file_path, file_name, file_size, duration, bit_rate, resolution, codec_name = file_data
                    size_str = format_file_size(file_size)
                    duration_str = format_duration(duration)
                    bitrate_str = format_bitrate(bit_rate)
                    codec_str = codec_name[:8] if codec_name else "N/A"
                    
                    is_matching = path_pattern is None or path_pattern in file_path
                    marker = " ← MATCHES PATTERN" if is_matching else ""
                    f.write(f"# COPY {j}: {size_str} | {duration_str} | {bitrate_str} | {resolution} | {codec_str}{marker}\n")
                    f.write(f"# {file_path}\n")
                
                f.write("#\n# Files to delete (copies matching pattern):\n")
                
                # Export only filtered copies for deletion
                for file_path, file_name, file_size, duration, bit_rate, resolution, codec_name in filtered_copies:
                    f.write(f"{file_path}\n")
                    total_files += 1
                
                f.write("#\n")
            
    conn.close()
    
    print(f"\n{Fore.GREEN}✅ Duplicate list exported to: {output_file}{Style.RESET_ALL}")
    print(f"Duplicate groups found: {len(groups)}")
    if duplicate_patterns:
        print(f"Duplicate patterns used: {', '.join(duplicate_patterns)}")
    if path_pattern:
        print(f"Filtered by pattern: '{path_pattern}'")
    print(f"Copy files to process: {total_files}")
    print(f"Format: {'Short (paths only)' if short_format else 'Full (with metadata)'}")
    print(f"Space that can be freed: {Fore.RED}{format_file_size(total_wasted_space)}{Style.RESET_ALL}")

def export_directory_structure(db_path, output_file, console_output=False, current_time=None):
    """
    Exports directory structure analysis to text file
    Shows nested directory structure with file counts and sizes
    """
    
    results = query_all_database(db_path, ['file_path', 'file_size', 'media_type'], include_corrupted=False)
    
    if not results:
        print(f"{Fore.YELLOW}No files found in database{Style.RESET_ALL}")
        return
    
    # Build directory tree structure
    dir_tree = defaultdict(lambda: {
        'files': [], # (file_path, file_size, media_type)
        'subdirs': set(),
        'stats': {
            'total_files': 0,
            'images': 0,
            'videos': 0,
            'other_files': 0,
            'total_size': 0,
            'subdirs_count': 0
        }
    })

    common_root_dir = os.path.commonpath([os.path.dirname(row[0]) for row in results])
    
    # Process each file
    for file_path, file_size, media_type in results:
        # Get directory path
        dir_path = os.path.dirname(file_path)
        
        # Add file to its directory
        dir_tree[dir_path]['files'].append((file_path, file_size, media_type))
        
        # Update statistics for this directory
        stats = dir_tree[dir_path]['stats']
        stats['total_files'] += 1
        stats['total_size'] += file_size if file_size else 0
        
        if media_type == 'image':
            stats['images'] += 1
        elif media_type == 'video':
            stats['videos'] += 1
        else:
            stats['other_files'] += 1
        
        # Build parent-child relationships
        current_path = dir_path
        while current_path and current_path != common_root_dir:
            parent_path = os.path.dirname(current_path)
            if parent_path == current_path:  # Root reached (/)
                break
            
            # Add current as subdir of parent
            dir_tree[parent_path]['subdirs'].add(current_path)
            current_path = parent_path
    
    # Calculate aggregate statistics (including subdirectories)
    def calculate_recursive_stats(dir_path):
        """Calculate total stats including all subdirectories"""
        stats = dir_tree[dir_path]['stats'].copy()
        
        for subdir in dir_tree[dir_path]['subdirs']:
            sub_stats = calculate_recursive_stats(subdir)
            stats['total_files'] += sub_stats['total_files']
            stats['images'] += sub_stats['images']
            stats['videos'] += sub_stats['videos']
            stats['other_files'] += sub_stats['other_files']
            stats['total_size'] += sub_stats['total_size']
        
        stats['subdirs_count'] = len(dir_tree[dir_path]['subdirs'])
        return stats
    
    # Get all directories sorted by path depth and name
    all_dirs = sorted(dir_tree.keys(), key=lambda x: (x.count(os.sep), x))
    
    # Function to display directory tree (unified for console and file output)
    def display_directory_tree(dir_path, output_file, depth=0):
        """
        Unified function to display directory tree
        
        Args:
            dir_path: Directory path to display
            depth: Nesting depth (for future indentation if needed)
            output_file: File object to write to (None for console output)
        """
        nonlocal exported_count, total_size_all
        
        # Calculate stats
        direct_stats = {
            'total_files': len(dir_tree[dir_path]['files']),
            'images': sum(1 for f in dir_tree[dir_path]['files'] if f[2] == 'image'),
            'videos': sum(1 for f in dir_tree[dir_path]['files'] if f[2] == 'video'),
            'other_files': sum(1 for f in dir_tree[dir_path]['files'] if f[2] == 'other'),
            'total_size': sum(f[1] for f in dir_tree[dir_path]['files']),
            'subdirs_count': len(dir_tree[dir_path]['subdirs'])
        }
        
        recursive_stats = calculate_recursive_stats(dir_path)
        
        exported_count += 1
        total_size_all += recursive_stats['total_size']
        
        # Format directory info
        subdirs_count = direct_stats['subdirs_count']
        direct_files = direct_stats['total_files']
        total_files = recursive_stats['total_files']
        total_size = recursive_stats['total_size']
        
        # Build file type breakdown for recursive stats
        type_parts = []
        if recursive_stats['images'] > 0:
            count = recursive_stats['images']
            label = f"{count} image{'s' if count != 1 else ''}"
            type_parts.append(f"{Fore.CYAN}{label}{Style.RESET_ALL}")
                
        if recursive_stats['videos'] > 0:
            count = recursive_stats['videos']
            label = f"{count} video{'s' if count != 1 else ''}"
            type_parts.append(f"{Fore.MAGENTA}{label}{Style.RESET_ALL}")
                
        if recursive_stats['other_files'] > 0:
            count = recursive_stats['other_files']
            label = f"{count} file{'s' if count != 1 else ''}"
            type_parts.append(f"{Fore.YELLOW}{label}{Style.RESET_ALL}")
        
        # Format size with colors
        size_str = format_file_size(total_size)
        if total_size > 1_000_000_000:  # > 1GB
            colored_size = f"{Fore.RED}{size_str}{Style.RESET_ALL}"
        elif total_size > 100_000_000:  # > 100MB
            colored_size = f"{Fore.YELLOW}{size_str}{Style.RESET_ALL}"
        else:
            colored_size = f"{Fore.GREEN}{size_str}{Style.RESET_ALL}"
        
        # Build description
        if total_files == 0:
            desc = f"{Fore.LIGHTBLACK_EX}[empty]{Style.RESET_ALL}"
        else:
            parts = []
            if subdirs_count > 0:
                parts.append(f"{subdirs_count} dir{'s' if subdirs_count != 1 else ''}")
            
            parts.extend(type_parts)
            parts.append(colored_size)
            desc = f"[{', '.join(parts)}]"
        
        # Format directory path with colors
        display_path = dir_path + "/"
        colored_path = f"{Fore.BLUE}{display_path}{Style.RESET_ALL}"

        output_file.write(f"{colored_path} {desc}\n")

        # Recursively process subdirectories in sorted order
        subdirs = sorted(dir_tree[dir_path]['subdirs'])
        for subdir in subdirs:
            display_directory_tree(subdir, output_file, depth + 1)
    
    # Only write to file if output_file is provided
    if output_file:
        if current_time is None:
            current_time = datetime.datetime.now()
            
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"# Directory Structure Analysis\n")
            f.write(f"# Created: {current_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total directories analyzed: {len(all_dirs)}\n")
            f.write("#\n")
            f.write("# Format: [subdirs count, files breakdown, total size]\n")
            f.write("# Legend: dir/ = directory, empty = no files, images/videos/files = media types\n")
            f.write("#" + "="*100 + "\n\n")
            
            exported_count = 0
            total_size_all = 0
            
            # Connect to database for root directory check
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Start from root directories (those without parents in our tree)
            root_dirs = []
            for dir_path in all_dirs:
                # Skip root "/" if it only contains subdirectories
                if dir_path == '/':
                    # Check if root has any direct files
                    direct_files_count = sum(1 for f in cursor.execute(
                        "SELECT file_path FROM media_files WHERE file_path LIKE '/[^/]*' AND file_path NOT LIKE '/%/%'"
                    ).fetchall())
                    if direct_files_count == 0:
                        continue  # Skip root if it has no direct files
                
                # Check if this directory has a parent in our tree
                has_parent_in_tree = False
                for potential_parent in all_dirs:
                    if potential_parent != dir_path and potential_parent != '/' and dir_path.startswith(potential_parent + os.sep):
                        has_parent_in_tree = True
                        break
                
                if not has_parent_in_tree:
                    root_dirs.append(dir_path)
            
            for root_dir in sorted(root_dirs):
                display_directory_tree(root_dir, StripAnsiWriter(f))

            f.write("\n" + "#" + "="*100 + "\n")
            f.write(f"# SUMMARY:\n")
            f.write(f"# Directories exported: {exported_count}\n")
            f.write(f"# Total size: {format_file_size(total_size_all)}\n")
        
            conn.close()
        
        if output_file:
            print(f"\n{Fore.GREEN}✅ Directory structure exported to: {output_file}{Style.RESET_ALL}")

    # Console output if requested
    if console_output:
        print(f"\n{Fore.CYAN}📁 Directory Structure:{Style.RESET_ALL}")
        
        # Reset counters for console output
        exported_count = 0
        total_size_all = 0

        # Display tree in console with colors
        display_directory_tree(common_root_dir, sys.stdout)
    
    # Output statistics to screen
    if output_file or console_output:
        if not console_output:  # Only show success message if file was written
            print()
        print(f"Directories analyzed: {len(all_dirs)}")
        if output_file:
            print(f"Directories exported: {exported_count}")
        
        # Calculate correct total size (sum of all unique files, not directories)
        unique_files_total_size = 0
        for file_path, file_size, media_type in results:
            unique_files_total_size += file_size
        
        print(f"Total size: {format_file_size(unique_files_total_size)}")
        
        # Show examples of largest directories
        print(f"\n{Fore.CYAN}Largest directories:{Style.RESET_ALL}")
        
    dir_sizes = []
    for dir_path in all_dirs:
        recursive_stats = calculate_recursive_stats(dir_path)
        dir_sizes.append((dir_path, recursive_stats))
    
    # Sort by total size descending
    dir_sizes.sort(key=lambda x: x[1]['total_size'], reverse=True)
    
    for i, (dir_path, stats) in enumerate(dir_sizes[:5]):
        size_str = format_file_size(stats['total_size'])
        files_str = f"{stats['total_files']} files"
        display_path = dir_path + "/" if dir_path else "<root>/"
        
        # Color code size
        if stats['total_size'] > 1_000_000_000:  # > 1GB
            colored_size = f"{Fore.RED}{size_str}{Style.RESET_ALL}"
        elif stats['total_size'] > 100_000_000:  # > 100MB
            colored_size = f"{Fore.YELLOW}{size_str}{Style.RESET_ALL}"
        else:
            colored_size = f"{Fore.GREEN}{size_str}{Style.RESET_ALL}"
        
        print(f"  {i+1}. {Fore.BLUE}{display_path}{Style.RESET_ALL} ({colored_size}, {files_str})")
    
    remaining = len(dir_sizes) - 5
    if remaining > 0:
        print(f"  ... and {remaining} more directories")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Media file database queries (videos and images)'
    )
    parser.add_argument(
        '--database', '-d',
        default='media_analysis.db',
        help='Path to SQLite database (default: media_analysis.db)'
    )
    parser.add_argument(
        '--export-list', '-e',
        metavar='FILE',
        help='Export file list to text file (required for all export operations)'
    )
    parser.add_argument(
        '--min-bitrate',
        type=int,
        metavar='MBPS',
        help='Export files with bitrate above given value in Mbit/s (for videos)'
    )
    parser.add_argument(
        '--suffix',
        metavar='SUFFIX',
        help='Export files with given suffix that have corresponding originals (e.g., _720p)'
    )
    parser.add_argument(
        '--export-no-metadata',
        action='store_true',
        help='Export files without creation_date metadata'
    )
    parser.add_argument(
        '--export-raw',
        action='store_true',
        help='Export RAW image files'
    )
    parser.add_argument(
        '--export-old-video',
        action='store_true',
        help='Export video files with outdated codecs/formats'
    )
    parser.add_argument(
        '--export-duplicates',
        action='store_true',
        help='Export duplicate files'
    )
    parser.add_argument(
        '--export-dirs',
        action='store_true',
        help='Export directory structure analysis'
    )
    parser.add_argument(
        '--export-corrupted',
        action='store_true',
        help='Export corrupted files (is_corrupted = 1)'
    )
    parser.add_argument(
        '--console',
        action='store_true',
        help='Display directory structure in console with colors (use with --export-dirs)'
    )
    parser.add_argument(
        '--export-pattern',
        metavar='PATTERN',
        nargs='+',
        help='Filter exports by path patterns (e.g., "Camera Uploads" "copy" "_copy")'
    )
    parser.add_argument(
        '--short', '-s',
        action='store_true',
        help='Export only file names (short format)'
    )
    parser.add_argument(
        '--now-time',
        metavar='DATETIME',
        help='Current time for deterministic output (format: YYYY-MM-DD HH:MM:SS)'
    )
    
    args = parser.parse_args()
    
    # Parse current time parameter for deterministic output
    if args.now_time:
        try:
            current_time = datetime.datetime.strptime(args.now_time, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            print(f"{Fore.RED}Error: Invalid --now-time format. Use: YYYY-MM-DD HH:MM:SS{Style.RESET_ALL}")
            return
    else:
        current_time = datetime.datetime.now()
    
    # Check database existence
    try:
        conn = sqlite3.connect(args.database)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM media_files")
        total_files = cursor.fetchone()[0]
        conn.close()
        
        if total_files == 0:
            print(f"{Fore.YELLOW}Database is empty. First run video file analysis.{Style.RESET_ALL}")
            return
            
        print(f"{Fore.GREEN}Database contains {total_files} records{Style.RESET_ALL}")
        
    except sqlite3.Error as e:
        print(f"{Fore.RED}Database access error: {e}{Style.RESET_ALL}")
        return
    
    # Check if export is requested
    if not args.export_list and not (args.export_dirs and args.console):
        # No export requested - show default reports
        query_largest_files(args.database, 20)
        query_high_bitrate_files(args.database, args.min_bitrate or 10, 20)
        query_longest_files(args.database, 20)
        return
    
    # Export operations require --export-list to be specified
    export_count = sum([
        bool(args.min_bitrate),
        bool(args.suffix),
        args.export_no_metadata,
        args.export_raw,
        args.export_old_video,
        args.export_duplicates,
        args.export_dirs,
        args.export_corrupted
    ])
    
    if export_count == 0:
        print(f"{Fore.RED}Error: --export-list specified but no export type selected{Style.RESET_ALL}")
        print("Available export types:")
        print("  --min-bitrate MBPS    Export files with high bitrate")
        print("  --suffix SUFFIX       Export files with suffix")
        print("  --export-no-metadata  Export files without metadata")
        print("  --export-raw          Export RAW image files")
        print("  --export-old-video    Export video files with outdated codecs/formats")
        print("  --export-duplicates   Export duplicate files")
        print("  --export-dirs         Export directory structure analysis")
        print("  --export-corrupted    Export corrupted files")
        return
    
    if export_count > 1:
        print(f"{Fore.RED}Error: Only one export type can be specified at a time{Style.RESET_ALL}")
        return
    
    # Perform the requested export
    if args.min_bitrate:
        # Use default min size of 50MB for high bitrate export
        export_files_list(args.database, args.export_list, args.min_bitrate, 50, args.short, current_time)
    elif args.suffix:
        export_files_with_suffix(args.database, args.export_list, args.suffix, args.short, current_time)
    elif args.export_no_metadata:
        export_no_metadata_files(args.database, args.export_list, args.short, current_time)
    elif args.export_raw:
        export_raw_files(args.database, args.export_list, args.short, current_time)
    elif args.export_old_video:
        export_old_video_files(args.database, args.export_list, args.short, current_time)
    elif args.export_duplicates:
        # Use first pattern for filtering, all patterns for duplicate detection
        filter_pattern = args.export_pattern[0] if args.export_pattern else None
        duplicate_patterns = args.export_pattern if args.export_pattern else None
        export_duplicates_list(args.database, args.export_list, filter_pattern, args.short, duplicate_patterns, current_time)
    elif args.export_dirs:
        output_file = args.export_list if args.export_list else None
        export_directory_structure(args.database, output_file, args.console, current_time)
    elif args.export_corrupted:
        export_corrupted_files(args.database, args.export_list, args.short, current_time)

if __name__ == "__main__":
    main()
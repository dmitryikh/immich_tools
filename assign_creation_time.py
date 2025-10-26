#!/usr/bin/env python3
"""
Restore Creation Time Metadata from File List with Suggested Timestamps

Processes files from a list exported by media_query.py --export-no-metadata 
and assigns creation time metadata based on CREATION_TIME suggestions in the file.

Expected input format (from media_query.py --export-no-metadata):
```
# VIDEO | 3.9 MB | 00:12 | 2.6 Mbit/s | 1280x720 | h264 | 2016-05-14 02:12:13
/data/homevideo/2016/2016.05.14 - Парк Патриот/alpha/00000_720p.mp4
# From path:
CREATION_TIME 2016-05-14 00:00:00

# VIDEO | 2.1 GB | 54:39 | 5.6 Mbit/s | 720x576 | h264 | 2025-10-21 07:25:29
/data/2006 - Выпускной .mp4
```

Usage:
python assign_creation_time.py files_with_suggestions.txt --pattern "Camera Uploads" --dry-run
"""

import os
import sys
import argparse
import re
from typing import Optional, List
from datetime import datetime
from pathlib import Path
from colorama import Fore, Style, init
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time
from tqdm import tqdm

# Import from local library
from lib.metadata import set_image_exif_datetime, set_video_metadata_datetime, get_image_metadata, get_video_metadata, VideoMetadataError
from lib.utils import IMAGE_EXTENSIONS, VIDEO_EXTENSIONS, SUPPORTED_EXTENSIONS

# Initialize colorama with forced colors for container support
init(autoreset=True, strip=False)

def parse_file_list_with_suggestions(input_file_path: str) -> List[tuple[str, Optional[datetime]]]:
    """
    Parse file list with CREATION_TIME suggestions from media_query.py --export-no-metadata
    
    Returns:
        List of tuples: (file_path, suggested_datetime)
    """
    results = []
    
    try:
        with open(input_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Skip comments and empty lines
            if line.startswith('#') or not line:
                i += 1
                continue
            
            # Skip CREATION_TIME lines (they should be processed with file paths)
            if line.startswith('CREATION_TIME '):
                i += 1
                continue
            
            # This should be a file path
            current_file_path = line
            suggested_datetime = None
            
            # Look for CREATION_TIME suggestion in the next few lines
            j = i + 1
            while j < len(lines) and j < i + 5:  # Look at most 5 lines ahead
                next_line = lines[j].strip()
                
                # Break if we hit another file path (doesn't start with # or CREATION_TIME)
                if (next_line and 
                    not next_line.startswith('#') and 
                    not next_line.startswith('CREATION_TIME')
                    ):
                    break
                
                # Check for CREATION_TIME pattern
                if next_line.startswith('CREATION_TIME '):
                    timestamp_str = next_line.replace('CREATION_TIME ', '').strip()
                    try:
                        suggested_datetime = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                    except ValueError as e:
                        print(f"{Fore.RED}Error: Invalid datetime format in line '{next_line}': {e}{Style.RESET_ALL}")
                        print(f"{Fore.RED}Expected format: CREATION_TIME YYYY-MM-DD HH:MM:SS{Style.RESET_ALL}")
                        sys.exit(1)
                    break
                
                j += 1
            
            results.append((current_file_path, suggested_datetime))
            i = j if j > i + 1 else i + 1
    
    except Exception as e:
        print(f"{Fore.RED}Error reading file list: {e}{Style.RESET_ALL}")
        return []
    
    return results

# Thread-safe counters
stats_lock = Lock()
stats = {
    'processed': 0,
    'updated': 0,
    'skipped_has_metadata': 0,
    'skipped_no_pattern': 0,
    'errors': 0
}

def filter_supported_media_files(file_suggestions: List[tuple[str, Optional[datetime]]]) -> List[tuple[str, Optional[datetime]]]:
    """Filter list to only include supported media files"""
    result_files = []
    
    for media_file_path, suggested_dt in file_suggestions:
        if not os.path.exists(media_file_path):
            continue  # Skip non-existent files
            
        file_extension = Path(media_file_path).suffix.lower()
        
        if file_extension in SUPPORTED_EXTENSIONS:
            result_files.append((media_file_path, suggested_dt))
    
    return result_files


def has_creation_metadata(file_path: str) -> bool:
    """Check if file already has creation time metadata"""
    try:
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext in IMAGE_EXTENSIONS:
            metadata = get_image_metadata(file_path)
            return 'creation_date' in metadata and metadata['creation_date']
        elif file_ext in VIDEO_EXTENSIONS:
            metadata = get_video_metadata(file_path)
            return metadata.get('creation_date') is not None
            
    except Exception:
        pass
    
    return False

def set_metadata_datetime(file_path: str, creation_time: datetime, dry_run: bool = False, prefer_metadata: bool = True, tools_available: dict = None) -> tuple[bool, str]:
    """
    Set datetime metadata for media files
    
    Args:
        file_path: Path to the media file
        creation_time: Datetime to set
        dry_run: If True, don't actually modify files
        prefer_metadata: If True, try to set file metadata first, then filesystem timestamp
        tools_available: Dict indicating which external tools are available
        
    Returns:
        tuple: (success: bool, method: str) - success status and method used
    """
    file_ext = Path(file_path).suffix.lower()
    
    # Set metadata based on file type
    if file_ext in IMAGE_EXTENSIONS:
        success = set_image_exif_datetime(file_path, creation_time, dry_run)
        if success:
            return True, "EXIF"
    elif file_ext in VIDEO_EXTENSIONS:
        success = set_video_metadata_datetime(file_path, creation_time, dry_run)
        if success:
            return True, "Video Metadata"
    
    # No suitable file type
    return False, "Unsupported file type"

def process_file(file_path: str, suggested_datetime: Optional[datetime], dry_run: bool = False, verbose: bool = False) -> str:
    """Process single file - check metadata and restore if suggested datetime is available"""
    global stats
    
    try:
        # Check if file has creation metadata
        if has_creation_metadata(file_path):
            with stats_lock:
                stats['processed'] += 1
                stats['skipped_has_metadata'] += 1
            
            if verbose:
                return f"{Fore.BLUE}SKIP (has metadata): {file_path}{Style.RESET_ALL}"
            return "skipped_has_metadata"
        
        # Check if we have a suggested datetime
        if not suggested_datetime:
            with stats_lock:
                stats['processed'] += 1
                stats['skipped_no_pattern'] += 1
            
            if verbose:
                return f"{Fore.YELLOW}SKIP (no suggestion): {file_path}{Style.RESET_ALL}"
            return "skipped_no_pattern"
        
        # Set creation time metadata using suggested datetime
        if dry_run:
            with stats_lock:
                stats['processed'] += 1
                stats['updated'] += 1
            
            file_ext = Path(file_path).suffix.lower()
            if file_ext in IMAGE_EXTENSIONS:
                method = "EXIF"
            elif file_ext in VIDEO_EXTENSIONS:
                method = "Video Metadata"
            else:
                method = "Unknown"
            
            return f"{Fore.CYAN}[DRY RUN] Would set {file_path} -> {suggested_datetime} (via {method}){Style.RESET_ALL}"
        else:
            success, method = set_metadata_datetime(file_path, suggested_datetime, dry_run)
            
            with stats_lock:
                stats['processed'] += 1
                if success:
                    stats['updated'] += 1
                else:
                    stats['errors'] += 1
            
            if success:
                return f"{Fore.GREEN}✓ UPDATED: {file_path} -> {suggested_datetime} (via {method}){Style.RESET_ALL}"
            else:
                return f"{Fore.RED}✗ ERROR: Failed to update {file_path}{Style.RESET_ALL}"
                
    except Exception as e:
        with stats_lock:
            stats['processed'] += 1
            stats['errors'] += 1
        
        return f"{Fore.RED}ERROR processing {file_path}: {e}{Style.RESET_ALL}"

def filter_media_files(file_list: List[str]) -> List[str]:
    """Filter list to only include supported media files"""
    media_files = []
    
    for file_path in file_list:
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext in SUPPORTED_EXTENSIONS:
            media_files.append(file_path)
    
    return media_files

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Restore creation time metadata from file list with suggested timestamps'
    )
    parser.add_argument(
        'file_list',
        help='Path to file with list of files and CREATION_TIME suggestions (from media_query.py --export-no-metadata)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Test mode: show what would be changed without making actual changes'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output with detailed processing information'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel workers for processing files (default: 4)'
    )
    parser.add_argument(
        '--pattern',
        help='Only process files containing specified pattern in path'
    )
    
    args = parser.parse_args()
    
    # Validate file list exists
    if not os.path.exists(args.file_list):
        print(f"{Fore.RED}❌ File list not found: {args.file_list}{Style.RESET_ALL}")
        sys.exit(1)
    
    # Read file list with suggestions
    print(f"📋 Reading list from: {args.file_list}")
    file_suggestions = parse_file_list_with_suggestions(args.file_list)
    
    if not file_suggestions:
        print(f"{Fore.YELLOW}⚠️  File list is empty{Style.RESET_ALL}")
        sys.exit(0)
    
    print(f"Found {len(file_suggestions)} paths in list")
    
    # Count files with suggestions
    with_suggestions = len([f for f in file_suggestions if f[1] is not None])
    print(f"Files with CREATION_TIME suggestions: {with_suggestions}")
    
    # Filter by pattern if specified
    if args.pattern:
        original_count = len(file_suggestions)
        file_suggestions = [(f, dt) for f, dt in file_suggestions if args.pattern in f]
        print(f"After pattern filtering '{args.pattern}': {len(file_suggestions)} of {original_count}")
    
    # Filter to only media files
    media_files = filter_supported_media_files(file_suggestions)
    
    if not media_files:
        print(f"{Fore.YELLOW}No media files found in list{Style.RESET_ALL}")
        sys.exit(0)
    
    print(f"{Fore.BLUE}Processing {len(media_files)} media files{Style.RESET_ALL}")
    
    if args.dry_run:
        print(f"{Fore.CYAN}Running in DRY RUN mode - no changes will be made{Style.RESET_ALL}")
    
    # Process files
    start_time = time.time()
    
    with tqdm(total=len(media_files), desc="Processing files", unit="files") as pbar:
        if args.workers > 1:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                future_to_file = {
                    executor.submit(process_file, file_path, suggested_datetime, args.dry_run, args.verbose): (file_path, suggested_datetime)
                    for file_path, suggested_datetime in media_files
                }
                
                for future in as_completed(future_to_file):
                    result = future.result()
                    
                    if args.verbose and not result.startswith("skipped"):
                        print(result)
                    
                    pbar.update(1)
        else:
            # Sequential processing
            for file_path, suggested_datetime in media_files:
                result = process_file(file_path, suggested_datetime, args.dry_run, args.verbose)
                
                if args.verbose and not result.startswith("skipped"):
                    print(result)
                
                pbar.update(1)
    
    # Display final statistics
    elapsed = time.time() - start_time
    # This line is not deterministic.
    # print(f"\n{Fore.GREEN}Processing completed in {elapsed:.2f} seconds!{Style.RESET_ALL}")
    print(f"Files processed: {stats['processed']}")
    print(f"Files updated: {stats['updated']}")
    print(f"Files with existing metadata: {stats['skipped_has_metadata']}")
    print(f"Files with no suggestions: {stats['skipped_no_pattern']}")
    print(f"Errors: {stats['errors']}")
    
    if args.dry_run:
        print(f"\n{Fore.CYAN}This was a dry run - no changes were made{Style.RESET_ALL}")
        print(f"{Fore.BLUE}Run without --dry-run to actually update file timestamps{Style.RESET_ALL}")

if __name__ == "__main__":
    main()
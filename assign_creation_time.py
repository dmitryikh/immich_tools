#!/usr/bin/env python3
"""
Restore Creation Time Metadata from File Paths

Processes files from a list and attempts to restore creation time metadata 
from file paths and filenames when EXIF data is missing.

Supported patterns:
- /path/2011/folder/file.mov -> 2011-01-01 00:00:00
- /path/2013/2013.06.xx - folder/file.MOV -> 2013-06-01 00:00:00  
- /path/2013/2013.09.13-folder/file.MOV -> 2013-09-13 00:00:00
- /path/2015/folder/2015-12-27 19-22-41.MP4 -> 2015-12-27 19:22:41

Usage:
python assign_creation_time.py files_list.txt --pattern "Camera Uploads" --dry-run
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
from lib.utils import IMAGE_EXTENSIONS, VIDEO_EXTENSIONS, SUPPORTED_EXTENSIONS, read_file_list, parse_datetime_from_path

# Initialize colorama with forced colors for container support
init(autoreset=True, strip=False)

# Thread-safe counters
stats_lock = Lock()
stats = {
    'processed': 0,
    'updated': 0,
    'updated_from_mtime': 0,
    'skipped_has_metadata': 0,
    'skipped_no_pattern': 0,
    'errors': 0
}


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

def process_file(file_path: str, dry_run: bool = False, verbose: bool = False, fallback_to_mtime: bool = False) -> str:
    """Process single file - check metadata and restore if needed"""
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
        
        # Try to parse datetime from path
        parsed_datetime = parse_datetime_from_path(file_path)
        fallback_used = False
        
        # If no pattern found and fallback is enabled, use mtime
        if not parsed_datetime and fallback_to_mtime:
            try:
                mtime = os.path.getmtime(file_path)
                parsed_datetime = datetime.fromtimestamp(mtime)
                fallback_used = True
            except (OSError, ValueError):
                parsed_datetime = None
        
        if not parsed_datetime:
            with stats_lock:
                stats['processed'] += 1
                stats['skipped_no_pattern'] += 1
            
            if verbose:
                return f"{Fore.YELLOW}SKIP (no pattern): {file_path}{Style.RESET_ALL}"
            return "skipped_no_pattern"
        
        # Set creation time metadata
        if dry_run:
            with stats_lock:
                stats['processed'] += 1
                stats['updated'] += 1
                if fallback_used:
                    stats['updated_from_mtime'] += 1
            
            file_ext = Path(file_path).suffix.lower()
            if file_ext in IMAGE_EXTENSIONS:
                method = "EXIF"
            elif file_ext in VIDEO_EXTENSIONS:
                method = "Video Metadata"
            else:
                method = "Unknown"
            
            fallback_text = " [from mtime]" if fallback_used else ""
            return f"{Fore.CYAN}[DRY RUN] Would set {file_path} -> {parsed_datetime} (via {method}){fallback_text}{Style.RESET_ALL}"
        else:
            success, method = set_metadata_datetime(file_path, parsed_datetime, dry_run)
            
            with stats_lock:
                stats['processed'] += 1
                if success:
                    stats['updated'] += 1
                    if fallback_used:
                        stats['updated_from_mtime'] += 1
                else:
                    stats['errors'] += 1
            
            fallback_text = " [from mtime]" if fallback_used else ""
            if success:
                return f"{Fore.GREEN}✓ UPDATED: {file_path} -> {parsed_datetime} (via {method}){fallback_text}{Style.RESET_ALL}"
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
        description='Restore creation time metadata from file paths and names'
    )
    parser.add_argument(
        'file_list',
        help='Path to file with list of files to process'
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
        '--image-only',
        action='store_true',
        help='Process only image files'
    )
    parser.add_argument(
        '--video-only',
        action='store_true',
        help='Process only video files'
    )
    parser.add_argument(
        '--pattern',
        help='Only process files containing specified pattern in path'
    )
    parser.add_argument(
        '--fallback-to-mtime',
        action='store_true',
        help='Use file modification time if creation date cannot be parsed from path'
    )
    
    args = parser.parse_args()
    
    # Validate file list exists
    if not os.path.exists(args.file_list):
        print(f"{Fore.RED}❌ File list not found: {args.file_list}{Style.RESET_ALL}")
        sys.exit(1)
    
    # Read file list
    print(f"📋 Reading list from: {args.file_list}")
    file_list = read_file_list(args.file_list)
    
    if not file_list:
        print(f"{Fore.YELLOW}⚠️  File list is empty{Style.RESET_ALL}")
        sys.exit(0)
    
    print(f"Found {len(file_list)} paths in list")
    
    # Filter by pattern if specified
    if args.pattern:
        original_count = len(file_list)
        file_list = [f for f in file_list if args.pattern in f]
        print(f"After pattern filtering '{args.pattern}': {len(file_list)} of {original_count}")
    
    # Filter to only media files
    media_files = filter_media_files(file_list)
    
    # Filter by type if requested
    if args.image_only:
        media_files = [f for f in media_files if Path(f).suffix.lower() in IMAGE_EXTENSIONS]
    elif args.video_only:
        media_files = [f for f in media_files if Path(f).suffix.lower() in VIDEO_EXTENSIONS]
    
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
                    executor.submit(process_file, file_path, args.dry_run, args.verbose, args.fallback_to_mtime): file_path 
                    for file_path in media_files
                }
                
                for future in as_completed(future_to_file):
                    result = future.result()
                    
                    if args.verbose and not result.startswith("skipped"):
                        print(result)
                    
                    pbar.update(1)
        else:
            # Sequential processing
            for file_path in media_files:
                result = process_file(file_path, args.dry_run, args.verbose, args.fallback_to_mtime)
                
                if args.verbose and not result.startswith("skipped"):
                    print(result)
                
                pbar.update(1)
    
    # Display final statistics
    elapsed = time.time() - start_time
    print(f"\n{Fore.GREEN}Processing completed in {elapsed:.2f} seconds!{Style.RESET_ALL}")
    print(f"Files processed: {stats['processed']}")
    print(f"Files updated: {stats['updated']}")
    if stats['updated_from_mtime'] > 0:
        print(f"Files updated from mtime: {stats['updated_from_mtime']}")
    print(f"Files with existing metadata: {stats['skipped_has_metadata']}")
    print(f"Files with no date pattern: {stats['skipped_no_pattern']}")
    print(f"Errors: {stats['errors']}")
    
    if args.dry_run:
        print(f"\n{Fore.CYAN}This was a dry run - no changes were made{Style.RESET_ALL}")
        print(f"{Fore.BLUE}Run without --dry-run to actually update file timestamps{Style.RESET_ALL}")

if __name__ == "__main__":
    main()
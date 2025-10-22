#!/usr/bin/env python3
"""
Restore Creation Time Metadata from File Paths

Recursively scans directories for video and image files and attempts to restore
creation time metadata from file paths and filenames when EXIF data is missing.

Supported patterns:
- /path/2011/folder/file.mov -> 2011-01-01 00:00:00
- /path/2013/2013.06.xx - folder/file.MOV -> 2013-06-01 00:00:00  
- /path/2013/2013.09.13-folder/file.MOV -> 2013-09-13 00:00:00
- /path/2015/folder/2015-12-27 19-22-41.MP4 -> 2015-12-27 19:22:41

Usage:
python restore_metadata_from_path.py /path/to/directory --dry-run
"""

import os
import sys
import argparse
import re
from typing import Optional, List, Tuple
from datetime import datetime
from pathlib import Path
from colorama import Fore, Style, init
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time
from tqdm import tqdm
from PIL import Image
from PIL.ExifTags import TAGS
import subprocess
import json

# Colorama init
init()

# Thread-safe counters
stats_lock = Lock()
stats = {
    'processed': 0,
    'updated': 0,
    'skipped_has_metadata': 0,
    'skipped_no_pattern': 0,
    'errors': 0
}

# Supported file extensions
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.heic', '.heif'}
VIDEO_EXTENSIONS = {'.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.m4v', '.3gp', '.mts', '.m2ts', '.mpg'}
ALL_EXTENSIONS = IMAGE_EXTENSIONS | VIDEO_EXTENSIONS

def parse_datetime_from_path(file_path: str) -> Optional[datetime]:
    """
    Extract datetime from file path and filename using various patterns
    
    Supported patterns:
    1. Year in directory: /path/2011/folder/ -> 2011-01-01 00:00:00
    2. Year.Month in folder: /2013/2013.06.xx - folder/ -> 2013-06-01 00:00:00
    3. Year.Month.Day in folder: /2013/2013.09.13-folder/ -> 2013-09-13 00:00:00
    4. Full datetime in filename: 2015-12-27 19-22-41.MP4 -> 2015-12-27 19:22:41
    5. Date in filename: 2018-03-10_21-30-06.JPG -> 2018-03-10 21:30:06
    """
    
    # Pattern 4 & 5: Full datetime in filename
    filename = os.path.basename(file_path)
    
    # Full datetime patterns in filename
    datetime_patterns = [
        # 2015-12-27 19-22-41.MP4
        r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2})-(\d{2})-(\d{2})',
        # 2015-12-27_19-22-41.MP4  
        r'(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})',
        # 2015-12-27T19:22:41.MP4
        r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})',
        # 20151227_192241.MP4
        r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})',
        # 2015-12-27 19:22:41.MP4
        r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})',
    ]
    
    for pattern in datetime_patterns:
        match = re.search(pattern, filename)
        if match:
            try:
                groups = match.groups()
                year = int(groups[0])
                month = int(groups[1])
                day = int(groups[2])
                hour = int(groups[3])
                minute = int(groups[4])
                second = int(groups[5])
                
                return datetime(year, month, day, hour, minute, second)
            except (ValueError, IndexError):
                continue
    
    # Pattern 3: Date in folder name (2013.09.13)
    path_parts = Path(file_path).parts
    for part in path_parts:
        # 2013.09.13-folder or 2013.09.13 - folder
        match = re.search(r'(\d{4})\.(\d{2})\.(\d{2})', part)
        if match:
            try:
                year = int(match.group(1))
                month = int(match.group(2))
                day = int(match.group(3))
                return datetime(year, month, day, 0, 0, 0)
            except ValueError:
                continue
    
    # Pattern 2: Year.Month in folder name (2013.06.xx)
    for part in path_parts:
        # 2013.06.xx - folder
        match = re.search(r'(\d{4})\.(\d{2})\.\w+', part)
        if match:
            try:
                year = int(match.group(1))
                month = int(match.group(2))
                return datetime(year, month, 1, 0, 0, 0)
            except ValueError:
                continue
    
    # Pattern 1: Year in directory path
    for part in path_parts:
        # Simple 4-digit year
        if re.match(r'^\d{4}$', part):
            try:
                year = int(part)
                if 1900 <= year <= 2030:  # Reasonable year range
                    return datetime(year, 1, 1, 0, 0, 0)
            except ValueError:
                continue
    
    return None

def has_creation_metadata(file_path: str) -> bool:
    """Check if file already has creation time metadata"""
    try:
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext in IMAGE_EXTENSIONS:
            return has_image_metadata(file_path)
        elif file_ext in VIDEO_EXTENSIONS:
            return has_video_metadata(file_path)
            
    except Exception:
        pass
    
    return False

def has_image_metadata(file_path: str) -> bool:
    """Check if image has EXIF creation date"""
    try:
        with Image.open(file_path) as img:
            exif = img._getexif()
            if exif:
                # Look for DateTime, DateTimeOriginal, DateTimeDigitized
                datetime_tags = [36867, 36868, 306]  # DateTimeOriginal, DateTimeDigitized, DateTime
                for tag in datetime_tags:
                    if tag in exif and exif[tag]:
                        return True
    except Exception:
        pass
    
    return False

def has_video_metadata(file_path: str) -> bool:
    """Check if video has creation date metadata using ffprobe"""
    try:
        cmd = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json', 
            '-show_entries', 'format_tags:stream_tags', file_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            return False
            
        data = json.loads(result.stdout)
        
        # Check format tags
        format_tags = data.get('format', {}).get('tags', {})
        if format_tags:
            creation_keys = ['creation_time', 'date', 'DATE']
            for key in creation_keys:
                if any(k.lower() == key.lower() for k in format_tags.keys()):
                    return True
        
        # Check stream tags
        streams = data.get('streams', [])
        for stream in streams:
            stream_tags = stream.get('tags', {})
            if stream_tags:
                creation_keys = ['creation_time', 'date', 'DATE']
                for key in creation_keys:
                    if any(k.lower() == key.lower() for k in stream_tags.keys()):
                        return True
                        
    except Exception:
        pass
    
    return False

def set_image_exif_datetime(file_path: str, creation_time: datetime, dry_run: bool = False) -> bool:
    """Set EXIF datetime for image files using exiftool"""
    try:
        if dry_run:
            return True
            
        # Format datetime for exiftool (YYYY:MM:DD HH:MM:SS)
        time_str = creation_time.strftime('%Y:%m:%d %H:%M:%S')
        
        # Use exiftool to set EXIF datetime tags
        cmd = [
            'exiftool', '-overwrite_original',
            f'-DateTimeOriginal={time_str}',
            f'-CreateDate={time_str}',
            f'-ModifyDate={time_str}',
            file_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return result.returncode == 0
        
    except Exception:
        return False

def set_video_metadata_datetime(file_path: str, creation_time: datetime, dry_run: bool = False) -> bool:
    """Set creation time metadata for video files using ffmpeg"""
    try:
        if dry_run:
            return True
            
        # Format datetime for ffmpeg (ISO 8601 format)
        time_str = creation_time.strftime('%Y-%m-%dT%H:%M:%S')
        
        # Create temporary output file
        temp_path = f"{file_path}.tmp"
        
        # Use ffmpeg to set metadata without re-encoding
        cmd = [
            'ffmpeg', '-i', file_path,
            '-c', 'copy',  # Copy without re-encoding
            '-metadata', f'creation_time={time_str}',
            '-metadata', f'date={time_str}',
            '-y',  # Overwrite output file
            temp_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            # Replace original file with updated one
            os.replace(temp_path, file_path)
            return True
        else:
            # Clean up temp file if it exists
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return False
        
    except Exception as e:
        # Clean up temp file if it exists
        temp_path = f"{file_path}.tmp"
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
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
    if tools_available is None:
        tools_available = {'ffmpeg': True, 'exiftool': True}
        
    file_ext = Path(file_path).suffix.lower()
    
    # Set metadata based on file type and available tools
    if file_ext in IMAGE_EXTENSIONS and tools_available.get('exiftool', False):
        success = set_image_exif_datetime(file_path, creation_time, dry_run)
        if success:
            return True, "EXIF"
    elif file_ext in VIDEO_EXTENSIONS and tools_available.get('ffmpeg', False):
        success = set_video_metadata_datetime(file_path, creation_time, dry_run)
        if success:
            return True, "Video Metadata"
    
    # No suitable metadata tool available
    return False, "No suitable tool available"

def process_file(file_path: str, dry_run: bool = False, verbose: bool = False, tools_available: dict = None) -> str:
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
            
            file_ext = Path(file_path).suffix.lower()
            if file_ext in IMAGE_EXTENSIONS and tools_available.get('exiftool', False):
                method = "EXIF"
            elif file_ext in VIDEO_EXTENSIONS and tools_available.get('ffmpeg', False):
                method = "Video Metadata"
            else:
                method = "No suitable tool"
            return f"{Fore.CYAN}[DRY RUN] Would set {file_path} -> {parsed_datetime} (via {method}){Style.RESET_ALL}"
        else:
            success, method = set_metadata_datetime(file_path, parsed_datetime, dry_run, True, tools_available)
            
            with stats_lock:
                stats['processed'] += 1
                if success:
                    stats['updated'] += 1
                else:
                    stats['errors'] += 1
            
            if success:
                return f"{Fore.GREEN}✓ UPDATED: {file_path} -> {parsed_datetime} (via {method}){Style.RESET_ALL}"
            else:
                return f"{Fore.RED}✗ ERROR: Failed to update {file_path}{Style.RESET_ALL}"
                
    except Exception as e:
        with stats_lock:
            stats['processed'] += 1
            stats['errors'] += 1
        
        return f"{Fore.RED}ERROR processing {file_path}: {e}{Style.RESET_ALL}"

def find_media_files(directory: str) -> List[str]:
    """Recursively find all media files in directory"""
    media_files = []
    
    print(f"{Fore.BLUE}Scanning directory: {directory}{Style.RESET_ALL}")
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_ext = Path(file_path).suffix.lower()
            
            if file_ext in ALL_EXTENSIONS:
                media_files.append(file_path)
    
    return media_files

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Restore creation time metadata from file paths and names'
    )
    parser.add_argument(
        'directory',
        help='Directory to scan recursively for media files'
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
    
    args = parser.parse_args()
    
    # Validate directory
    if not os.path.isdir(args.directory):
        print(f"{Fore.RED}Error: Directory '{args.directory}' does not exist{Style.RESET_ALL}")
        sys.exit(1)
    
    # Check for required tools
    tools_available = {
        'ffprobe': False,
        'ffmpeg': False,
        'exiftool': False
    }
    
    # Check ffprobe
    try:
        subprocess.run(['ffprobe', '-version'], capture_output=True, check=True, timeout=5)
        tools_available['ffprobe'] = True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        print(f"{Fore.YELLOW}Warning: ffprobe not found. Video metadata detection may not work properly.{Style.RESET_ALL}")
    
    # Check ffmpeg  
    try:
        subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True, timeout=5)
        tools_available['ffmpeg'] = True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        print(f"{Fore.YELLOW}Warning: ffmpeg not found. Video metadata writing will be disabled.{Style.RESET_ALL}")
    
    # Check exiftool
    try:
        subprocess.run(['exiftool', '-ver'], capture_output=True, check=True, timeout=5)
        tools_available['exiftool'] = True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        print(f"{Fore.YELLOW}Warning: exiftool not found. Image EXIF writing will be disabled.{Style.RESET_ALL}")
    
    if not any(tools_available.values()):
        print(f"{Fore.RED}Error: No metadata tools found. Cannot proceed without ffmpeg or exiftool.{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}Install ffmpeg and exiftool for metadata support.{Style.RESET_ALL}")
        sys.exit(1)
    
    # Find media files
    media_files = find_media_files(args.directory)
    
    # Filter by type if requested
    if args.image_only:
        media_files = [f for f in media_files if Path(f).suffix.lower() in IMAGE_EXTENSIONS]
    elif args.video_only:
        media_files = [f for f in media_files if Path(f).suffix.lower() in VIDEO_EXTENSIONS]
    
    if not media_files:
        print(f"{Fore.YELLOW}No media files found in directory{Style.RESET_ALL}")
        sys.exit(0)
    
    print(f"{Fore.BLUE}Found {len(media_files)} media files to process{Style.RESET_ALL}")
    
    if args.dry_run:
        print(f"{Fore.CYAN}Running in DRY RUN mode - no changes will be made{Style.RESET_ALL}")
    
    # Process files
    start_time = time.time()
    
    with tqdm(total=len(media_files), desc="Processing files", unit="files") as pbar:
        if args.workers > 1:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                future_to_file = {
                    executor.submit(process_file, file_path, args.dry_run, args.verbose, tools_available): file_path 
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
                result = process_file(file_path, args.dry_run, args.verbose, tools_available)
                
                if args.verbose and not result.startswith("skipped"):
                    print(result)
                
                pbar.update(1)
    
    # Display final statistics
    elapsed = time.time() - start_time
    print(f"\n{Fore.GREEN}Processing completed in {elapsed:.2f} seconds!{Style.RESET_ALL}")
    print(f"Files processed: {stats['processed']}")
    print(f"Files updated: {stats['updated']}")
    print(f"Files with existing metadata: {stats['skipped_has_metadata']}")
    print(f"Files with no date pattern: {stats['skipped_no_pattern']}")
    print(f"Errors: {stats['errors']}")
    
    if args.dry_run:
        print(f"\n{Fore.CYAN}This was a dry run - no changes were made{Style.RESET_ALL}")
        print(f"{Fore.BLUE}Run without --dry-run to actually update file timestamps{Style.RESET_ALL}")

if __name__ == "__main__":
    main()
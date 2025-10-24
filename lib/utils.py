#!/usr/bin/env python3
"""
Utility functions for photo converter
Common helper functions for file operations, formatting, and logging
"""

import os
import logging
import sqlite3
import unicodedata
from pathlib import Path
from colorama import Fore, Style

# Media file extensions
# Supported video formats
VIDEO_EXTENSIONS = {
    '.mp4', '.avi', '.mkv', '.mov', '.mod', '.wmv', '.flv', '.webm', 
    '.m4v', '.3gp', '.ogv', '.f4v', '.asf', '.rm', '.rmvb',
    '.vob', '.ts', '.mts', '.m2ts', '.mpg', '.mpeg', '.m2v'
}

# RAW image formats
RAW_EXTENSIONS = {
    '.raw', '.dng', '.cr2', '.cr3', '.nef', '.arw', '.orf', 
    '.rw2', '.pef', '.srw', '.raf', '.3fr', '.ari', '.srf', 
    '.sr2', '.bay', '.crw', '.erf', '.mef', '.mrw', '.nrw', 
    '.rwl', '.rwz', '.x3f'
}

# Supported image formats (regular + RAW)
IMAGE_EXTENSIONS = {
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', 
    '.webp', '.heic', '.heif'
} | RAW_EXTENSIONS

# All supported formats
SUPPORTED_EXTENSIONS = VIDEO_EXTENSIONS | IMAGE_EXTENSIONS

def setup_logging(log_file="photo_converter.log", log_level=logging.INFO):
    """Sets up logging to file and console"""
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Setup main logger
    logger = logging.getLogger('photo_converter')
    logger.setLevel(log_level)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler (WARNING and above only)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def read_file_list(file_path):
    """Reads list of files from text file"""
    files = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                line = unicodedata.normalize("NFC", line)
                # Skip comments and empty lines
                if line and not line.startswith('#'):
                    files.append(line)
        return files
    except FileNotFoundError:
        print(f"{Fore.RED}❌ File not found: {file_path}{Style.RESET_ALL}")
        return []
    except Exception as e:
        print(f"{Fore.RED}❌ Error reading file: {e}{Style.RESET_ALL}")
        return []

def format_file_size(size_bytes):
    """Formats file size in human readable format"""
    if size_bytes is None:
        return "N/A"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"

def format_duration(seconds):
    """Formats duration in MM:SS format"""
    if seconds is None:
        return "N/A"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes:02d}:{secs:02d}"

def get_output_path(original_path, suffix="_jpg", preserve_extension=False):
    """
    Generates output file path with suffix
    
    Args:
        original_path: Path to original file
        suffix: Suffix to add to filename
        preserve_extension: If True, keep original extension; if False, change to .jpg
    """
    path_obj = Path(original_path)
    
    if preserve_extension:
        # Keep original extension
        new_name = f"{path_obj.stem}{suffix}{path_obj.suffix}"
    else:
        # Change extension to .jpg (for photo conversion)
        new_name = f"{path_obj.stem}{suffix}.jpg"
        
    output_path = path_obj.parent / new_name
    return str(output_path)

def log_conversion_operation(logger, input_path, output_path, success, original_size=0, 
                           output_size=0, duration_seconds=0, error_msg=None, image_info=None):
    """Logs conversion operation"""
    if success:
        compression_percent = ((original_size - output_size) / original_size * 100) if original_size > 0 else 0
        info_str = f"{image_info['width']}x{image_info['height']}" if image_info else "Unknown"
        logger.info(
            f"CONVERT_SUCCESS: {input_path} -> {output_path} | "
            f"Size: {format_file_size(original_size)} -> {format_file_size(output_size)} "
            f"(-{compression_percent:.1f}%) | Resolution: {info_str} | Duration: {format_duration(duration_seconds)}"
        )
    else:
        logger.error(f"CONVERT_FAILED: {input_path} -> {output_path} | Error: {error_msg}")

def sort_files_by_directory_depth(files_list):
    """
    Sorts files by directory structure: subdirectories first (lexicographically), then files in parent directory
    
    This function implements a sorting algorithm that ensures proper file ordering:
    1. Files in deeper directories come first
    2. Within the same depth, directories are sorted lexicographically
    3. Within the same directory, files are sorted lexicographically
    
    Example output order:
    - /media/A/video1.mp4
    - /media/A/video2.mp4  
    - /media/B/video1.mp4
    - /media/avideo.mp4
    
    Args:
        files_list: List of tuples where first element contains file_path
                   Supports various tuple formats:
                   - (file_record, other_data) - where file_record[0] is file_path
                   - ((file_record, ...), other_data) - where file_record[0] is file_path
                   - Direct file records where item[0] is file_path
    
    Returns:
        Sorted list using the same structure as input
    """
    def sort_key(item):
        # Handle different input formats - extract file_path from first element
        if isinstance(item, tuple) and len(item) >= 1:
            if isinstance(item[0], (list, tuple)) and len(item[0]) >= 1:
                # Format: ((file_record, ...), other_data) - used in export_files_with_suffix
                file_path = item[0][0]
            else:
                # Format: (file_record, other_data) - used in other functions
                # or: file_record directly
                if isinstance(item[0], (list, tuple)) and len(item[0]) >= 1:
                    file_path = item[0][0]
                else:
                    file_path = item[0]
        else:
            # Direct file record format or string
            file_path = item[0] if isinstance(item, (list, tuple)) else str(item)
        
        dir_name = os.path.dirname(file_path)
        file_name = os.path.basename(file_path)
        
        # Count directory separators to determine depth
        dir_depth = dir_name.count(os.sep) if dir_name else 0
        
        # Sort by: reverse depth (deeper directories first), then directory name, then filename
        # Using negative depth to sort deeper directories first
        return (-dir_depth, dir_name, file_name)
    
    return sorted(files_list, key=sort_key)


def parse_datetime_from_path(file_path: str):
    """
    Extract datetime from file path and filename using various patterns
    
    Supported patterns:
    1. Year in directory: /path/2011/folder/ -> 2011-01-01 00:00:00
    2. Year.Month in folder: /2013/2013.06.xx - folder/ -> 2013-06-01 00:00:00
    3. Year.Month.Day in folder: /2013/2013.09.13-folder/ -> 2013-09-13 00:00:00
    4. Full datetime in filename: 2015-12-27 19-22-41.MP4 -> 2015-12-27 19:22:41
    5. Date in filename: 2018-03-10_21-30-06.JPG -> 2018-03-10 21:30:06
    
    Returns:
        datetime object if pattern found, None otherwise
    """
    import re
    from datetime import datetime
    
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


def load_database_file_paths(db_path):
    """
    Loads all file paths from the database into a set for fast lookup
    
    Args:
        db_path: Path to SQLite database
        
    Returns:
        set: Set of all file paths in the database
        
    Raises:
        ValueError: If database path is invalid or doesn't exist
        sqlite3.Error: If database query fails
    """
    if not db_path:
        raise ValueError("Database path cannot be None or empty")
    
    if not os.path.exists(db_path):
        raise ValueError(f"Database file does not exist: {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT file_path FROM media_files')
        file_paths = {row[0] for row in cursor.fetchall()}
        
        conn.close()
        return file_paths
        
    except sqlite3.Error as e:
        raise sqlite3.Error(f"Failed to query database {db_path}: {e}")


class DatabaseProtectionError(Exception):
    """Raised when trying to overwrite a file that exists in the database"""
    pass
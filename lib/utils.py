#!/usr/bin/env python3
"""
Utility functions for photo converter
Common helper functions for file operations, formatting, and logging
"""

import os
import logging
import unicodedata
from pathlib import Path
from colorama import Fore, Style

# Media file extensions
# Supported video formats
VIDEO_EXTENSIONS = {
    '.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', 
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

def get_output_path(original_path, suffix="_jpg"):
    """Generates output file path (adds suffix and changes extension to .jpg)"""
    path_obj = Path(original_path)
    # Create new name with suffix
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
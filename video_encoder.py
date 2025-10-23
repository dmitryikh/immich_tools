#!/usr/bin/env python3
"""
Mass video encoding script using FFmpeg Docker container
Creates optimized video files with configurable suffix

Usage:
    python3 video_encoder.py --suffix=_720p --dry-run high_quality_files.txt
"""

import os
import sys
import subprocess
import unicodedata
import argparse
import logging
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from colorama import Fore, Style, init

# Import local modules
from lib.video_converter import encode_video_file, get_output_path

# Initialize colorama with forced colors for container support
init(autoreset=True, strip=False)

def setup_logging(log_file="video_encoder.log", log_level=logging.INFO):
    """Sets up logging to file and console"""
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Setup main logger
    logger = logging.getLogger('video_encoder')
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

def log_encoding_operation(logger, input_path, output_path, success, original_size=0, 
                         output_size=0, duration_seconds=0, error_msg=None):
    """Logs encoding operation"""
    if success:
        compression_percent = ((original_size - output_size) / original_size * 100) if original_size > 0 else 0
        logger.info(
            f"ENCODE_SUCCESS: {input_path} -> {output_path} | "
            f"Size: {format_file_size(original_size)} -> {format_file_size(output_size)} "
            f"(-{compression_percent:.1f}%) | Duration: {format_duration(duration_seconds)} | Method: Docker"
        )
    else:
        logger.error(f"ENCODE_FAILED: {input_path} -> {output_path} | Error: {error_msg} | Method: Docker")

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

def check_ffmpeg():
    """Checks FFmpeg availability via Docker"""
    try:
        # First check Docker
        result = subprocess.run(['docker', '--version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            print(f"{Fore.RED}❌ Docker not found{Style.RESET_ALL}")
            return False
        
        # Check FFmpeg in Docker container
        result = subprocess.run([
            'docker', 'run', '--rm', 
            'linuxserver/ffmpeg:latest', 
            'ffmpeg', '-version'
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print(f"{Fore.GREEN}✅ FFmpeg Docker container ready{Style.RESET_ALL}")
            return True
        else:
            print(f"{Fore.YELLOW}⚠️ Downloading FFmpeg Docker image...{Style.RESET_ALL}")
            # Try to pull the image
            pull_result = subprocess.run([
                'docker', 'pull', 'linuxserver/ffmpeg:latest'
            ], capture_output=True, text=True, timeout=120)
            return pull_result.returncode == 0
            
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False

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
    """Formats duration in HH:MM:SS format"""
    if seconds is None:
        return "N/A"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    else:
        return f"{minutes:02d}:{secs:02d}"



def encode_video(input_path, output_path, logger, dry_run=True):
    """Encodes single video file - wrapper around lib.video_converter.encode_video_file"""
    
    # Handle dry-run with UI output
    if dry_run:
        print(f"  {Fore.CYAN}[DRY-RUN]{Style.RESET_ALL} Encode: {input_path} -> {os.path.basename(output_path)}")
        result = encode_video_file(input_path, output_path, dry_run=True)
        # Log dry-run operation
        log_encoding_operation(
            logger, input_path, output_path, True,
            result['original_size'], result['output_size'], 0
        )
        return result
    
    # Actual encoding
    result = encode_video_file(input_path, output_path, dry_run=False)
    
    # Add UI feedback and logging for real encoding
    if result['success']:
        temp_size_str = format_file_size(result['output_size'])
        print(f"  {Fore.GREEN}🔄{Style.RESET_ALL} Encoding completed: {temp_size_str}")
        
        # Log successful encoding
        log_encoding_operation(logger, input_path, output_path, True, 
                             result['original_size'], result['output_size'], result['duration'])
    else:
        # Log failed encoding
        log_encoding_operation(logger, input_path, output_path, False, 
                             result['original_size'], 0, result['duration'], result['error'])
    
    return result

def process_file_list(file_list, logger, suffix="_encoded", 
                     dry_run=True, skip_existing=True):
    """Processes list of files"""
    
    if dry_run:
        print(f"{Fore.YELLOW}🔍 PREVIEW MODE (files will NOT be processed){Style.RESET_ALL}")
    else:
        print(f"{Fore.GREEN}🎬 ENCODING MODE{Style.RESET_ALL}")
    
    print(f"Suffix for encoded files: {suffix}")
    print(f"Skip existing files: {skip_existing}")
    print("-" * 80)
    
    tasks = []
    skipped_count = 0
    
    # Prepare tasks
    for file_path in file_list:
        if not os.path.exists(file_path):
            print(f"{Fore.YELLOW}⚠️  Skipped (not found): {file_path}{Style.RESET_ALL}")
            continue
        
        output_path = get_output_path(file_path, suffix)
        
        # Check if we need to skip
        if skip_existing and os.path.exists(output_path):
            print(f"{Fore.BLUE}⏭️  Skipped (already exists): {os.path.basename(output_path)}{Style.RESET_ALL}")
            skipped_count += 1
            continue
        
        tasks.append((file_path, output_path))
    
    if not tasks:
        print(f"{Fore.YELLOW}❌ No files to process{Style.RESET_ALL}")
        return
    
    print(f"\nProcessing {len(tasks)} files (skipped: {skipped_count}):")
    
    # Statistics
    total_original_size = 0
    total_output_size = 0
    success_count = 0
    error_count = 0
    
    # Process files
    for i, (input_path, output_path) in enumerate(tasks, 1):
        print(f"\n[{i}/{len(tasks)}] {Fore.CYAN}{input_path}{Style.RESET_ALL}")
        
        result = encode_video(input_path, output_path, logger, dry_run)
        
        if result['success']:
            original_size_str = format_file_size(result['original_size'])
            output_size_str = format_file_size(result['output_size'])
            compression = ((result['original_size'] - result['output_size']) / result['original_size'] * 100) if result['original_size'] > 0 else 0
            
            print(f"  {Fore.GREEN}✅ Success{Style.RESET_ALL}: {original_size_str} → {output_size_str} (-{compression:.1f}%)")
            
            if not dry_run and result['duration'] > 0:
                print(f"  ⏱️  Duration: {format_duration(result['duration'])}")
            
            success_count += 1
            total_original_size += result['original_size']
            total_output_size += result['output_size']
        else:
            print(f"  {Fore.RED}❌ Error: {result['error']}{Style.RESET_ALL}")
            error_count += 1
    
    # Final statistics
    print("\n" + "=" * 80)
    print(f"{Fore.CYAN}📊 FINAL STATISTICS:{Style.RESET_ALL}")
    print(f"  Successfully processed: {Fore.GREEN}{success_count}{Style.RESET_ALL}")
    print(f"  Errors: {Fore.RED}{error_count}{Style.RESET_ALL}")
    print(f"  Skipped: {skipped_count}")
    
    if total_original_size > 0:
        compression_percent = ((total_original_size - total_output_size) / total_original_size * 100)
        print(f"  Original size: {format_file_size(total_original_size)}")
        if dry_run:
            print(f"  Expected size: {format_file_size(total_output_size)} (-{compression_percent:.1f}%)")
        else:
            print(f"  Final size: {format_file_size(total_output_size)} (-{compression_percent:.1f}%)")
            print(f"  Space saved: {Fore.GREEN}{format_file_size(total_original_size - total_output_size)}{Style.RESET_ALL}")

def main():
    parser = argparse.ArgumentParser(
        description='Mass video encoding using FFmpeg via Docker',
        epilog='Creates compressed copies of files with specified suffix'
    )
    parser.add_argument(
        'file_list',
        help='Path to file containing list of video files to process'
    )
    parser.add_argument(
        '--suffix', '-s',
        default='_encoded',
        help='Suffix for encoded files (default: _encoded)'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Preview mode - show what would be done without executing'
    )
    parser.add_argument(
        '--no-skip-existing',
        action='store_true',
        help='Do not skip files that already exist'
    )
    parser.add_argument(
        '--pattern',
        help='Only process files containing specified pattern in path'
    )
    
    args = parser.parse_args()
    
    # Check FFmpeg (via Docker)
    if not args.dry_run and not check_ffmpeg():
        print(f"{Fore.RED}❌ FFmpeg Docker container unavailable{Style.RESET_ALL}")
        print("Install Docker: https://docs.docker.com/get-docker/")
        print("Or pull FFmpeg image: docker pull linuxserver/ffmpeg:latest")
        return 1
    
    # Check file list
    if not os.path.exists(args.file_list):
        print(f"{Fore.RED}❌ File list not found: {args.file_list}{Style.RESET_ALL}")
        return 1
    
    # Read file list
    print(f"📋 Reading list from: {args.file_list}")
    file_list = read_file_list(args.file_list)
    
    if not file_list:
        print(f"{Fore.YELLOW}⚠️  File list is empty{Style.RESET_ALL}")
        return 1
    
    print(f"Found {len(file_list)} paths in list")
    
    # Filter by pattern if specified
    if args.pattern:
        original_count = len(file_list)
        file_list = [f for f in file_list if args.pattern in f]
        print(f"After pattern filtering '{args.pattern}': {len(file_list)} of {original_count}")
    
    if not file_list:
        print(f"{Fore.YELLOW}⚠️  List is empty after filtering{Style.RESET_ALL}")
        return 1
    
    # Setup logging
    logger = setup_logging()
    
    # Start processing
    try:
        process_file_list(
            file_list,
            logger,
            suffix=args.suffix,
            dry_run=args.dry_run,
            skip_existing=not args.no_skip_existing
        )
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}⚠️  Processing interrupted by user{Style.RESET_ALL}")
        logger.warning("Processing interrupted by user")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
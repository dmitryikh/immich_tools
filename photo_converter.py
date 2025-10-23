#!/usr/bin/env python3
"""
Mass RAW photo conversion script using RawTherapee CLI with parallel processing
Converts RAW images to high-quality JPEG while preserving resolution and all metadata

Uses RawTherapee CLI via Docker for professional RAW processing with automatic metadata preservation.
Features parallel processing and real-time progress bars for efficient batch conversion.
Requires immich_tools Docker image with RawTherapee CLI installed.

Usage:
    python photo_converter.py --pattern ".RW2" --no-skip-existing --max-workers 8 --quality 85 --suffix ""  tmp/files_without_creation_date.txt
"""

import os
import sys
import shutil
import time
import argparse
import tempfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from colorama import Fore, Style, init
from tqdm import tqdm

from lib.raw_converter import convert_raw_image_rawtherapee, is_raw_file, check_rawtherapee_dependencies
from lib.utils import (
    setup_logging, read_file_list, format_file_size, format_duration, 
    get_output_path, log_conversion_operation
)

# Initialize colorama with forced colors for container support
init(autoreset=True, strip=False)

def convert_image_worker(input_path, output_path, quality=95, logger=None, dry_run=True):
    """Thread-safe worker function for converting single image file"""
    result = {
        'input_path': input_path,
        'output_path': output_path,
        'success': False,
        'error': None,
        'original_size': 0,
        'output_size': 0,
        'duration': 0,
        'image_info': None,
        'messages': []  # Collect messages for thread-safe output
    }
    
    try:
        # Get original file size
        if os.path.exists(input_path):
            result['original_size'] = os.path.getsize(input_path)
        
        if dry_run:
            result['success'] = True
            result['output_size'] = result['original_size'] * 0.3  # Estimated compression
            result['image_info'] = {'width': 'Unknown', 'height': 'Unknown'}
            result['messages'].append(f"[DRY-RUN] Would convert: {os.path.basename(input_path)}")
            
            # Log dry-run operation
            if logger:
                log_conversion_operation(
                    logger, input_path, output_path, True,
                    result['original_size'], result['output_size'], 0, 
                    image_info=result['image_info']
                )
            return result
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)
        
        # Create temporary file in the same directory as final file
        temp_fd, temp_path = tempfile.mkstemp(
            suffix='.jpg',
            dir=output_dir,
            prefix=f"{Path(output_path).stem}_"
        )
        os.close(temp_fd)  # Close file descriptor
        
        try:
            # Start conversion
            start_time = time.time()
            
            # Only process RAW files
            if not is_raw_file(input_path):
                result['error'] = f"Not a RAW file: {input_path}"
                return result
                
            # Use RawTherapee CLI for RAW files (suppress output for parallel processing)
            image_info = convert_raw_image_rawtherapee(input_path, temp_path, quality, logger)
            
            result['duration'] = time.time() - start_time
            result['image_info'] = image_info
            
            if os.path.exists(temp_path):
                # Atomically move temporary file to final location
                shutil.move(temp_path, output_path)
                result['output_size'] = os.path.getsize(output_path)
                result['success'] = True
                
                result['messages'].append(f"Converted: {input_path} -> {os.path.basename(output_path)}")
                
                # Log successful conversion
                if logger:
                    log_conversion_operation(logger, input_path, output_path, True, 
                                           result['original_size'], result['output_size'], 
                                           result['duration'], image_info=result['image_info'])
            else:
                result['error'] = "Temporary file not created"
                if logger:
                    log_conversion_operation(logger, input_path, output_path, False, 
                                           result['original_size'], 0, result['duration'], 
                                           result['error'])
        
        finally:
            # Clean up temporary file if it remains
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass  # Ignore deletion errors
            
    except Exception as e:
        result['error'] = str(e)
        if logger:
            log_conversion_operation(logger, input_path, output_path, False, 
                                   result['original_size'], 0, result.get('duration', 0), 
                                   result['error'])
    
    return result

def process_file_list(file_list, logger, suffix="_jpg", quality=95,
                     dry_run=True, skip_existing=True, pattern=None, max_workers=4):
    """Processes list of files with parallel processing and progress bar"""
    
    if dry_run:
        print(f"{Fore.YELLOW}🔍 PREVIEW MODE (files will NOT be processed){Style.RESET_ALL}")
    else:
        print(f"{Fore.GREEN}📷 CONVERSION MODE{Style.RESET_ALL}")
    
    print(f"Suffix for converted files: {suffix}")
    print(f"JPEG Quality: {quality}")
    print(f"Skip existing files: {skip_existing}")
    print(f"Max workers: {max_workers}")
    if pattern:
        print(f"Pattern filter: {pattern}")
    print("-" * 80)
    
    tasks = []
    skipped_count = 0
    filtered_count = 0
    
    # Prepare tasks
    for file_path in file_list:
        if not os.path.exists(file_path):
            print(f"{Fore.YELLOW}⚠️  Skipped (not found): {file_path}{Style.RESET_ALL}")
            continue
        
        # Skip non-RAW files
        if not is_raw_file(file_path):
            print(f"{Fore.YELLOW}⚠️  Skipped (not a RAW file): {os.path.basename(file_path)}{Style.RESET_ALL}")
            filtered_count += 1
            continue
        
        # Apply pattern filter if specified
        if pattern and pattern not in file_path:
            filtered_count += 1
            continue
        
        output_path = get_output_path(file_path, suffix)
        
        # Check if we need to skip
        if skip_existing and os.path.exists(output_path):
            print(f"{Fore.BLUE}⏭️  Skipped (already exists): {os.path.basename(output_path)}{Style.RESET_ALL}")
            skipped_count += 1
            continue
        
        tasks.append((file_path, output_path))
    
    if filtered_count > 0:
        print(f"Filtered out {filtered_count} files not matching criteria")
    
    if not tasks:
        print(f"{Fore.YELLOW}❌ No files to process{Style.RESET_ALL}")
        return
    
    print(f"\nProcessing {len(tasks)} files (skipped: {skipped_count}):")
    
    # Statistics
    total_original_size = 0
    total_output_size = 0
    success_count = 0
    error_count = 0
    raw_count = 0
    
    # Process files with parallel processing and progress bar
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_task = {
            executor.submit(convert_image_worker, input_path, output_path, quality, logger, dry_run): (input_path, output_path)
            for input_path, output_path in tasks
        }
        
        # Process completed tasks with progress bar
        with tqdm(total=len(tasks), desc="Converting", unit="file", 
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
            
            for future in as_completed(future_to_task):
                input_path, output_path = future_to_task[future]
                
                try:
                    result = future.result()
                    
                    if result['success']:
                        original_size_str = format_file_size(result['original_size'])
                        output_size_str = format_file_size(result['output_size'])
                        compression = ((result['original_size'] - result['output_size']) / result['original_size'] * 100) if result['original_size'] > 0 else 0
                        
                        info_str = ""
                        if result['image_info']:
                            info_str = f" ({result['image_info']['width']}x{result['image_info']['height']})"
                        
                        # Update progress bar description
                        pbar.set_postfix_str(f"{Fore.GREEN}✓{Style.RESET_ALL} {os.path.basename(input_path)}")
                        
                        success_count += 1
                        total_original_size += result['original_size']
                        total_output_size += result['output_size']
                        raw_count += 1  # All processed files are RAW files
                        
                        # Print messages from worker if any
                        if result['messages']:
                            tqdm.write(f"  {' '.join(result['messages'])}")
                        
                    else:
                        pbar.set_postfix_str(f"{Fore.RED}✗{Style.RESET_ALL} {os.path.basename(input_path)}")
                        tqdm.write(f"  {Fore.RED}❌ Error: {result['error']}{Style.RESET_ALL}")
                        error_count += 1
                        
                except Exception as exc:
                    pbar.set_postfix_str(f"{Fore.RED}✗{Style.RESET_ALL} {os.path.basename(input_path)}")
                    tqdm.write(f"  {Fore.RED}❌ Exception: {exc}{Style.RESET_ALL}")
                    error_count += 1
                
                pbar.update(1)
    
    # Final statistics
    print("\n" + "=" * 80)
    print(f"{Fore.CYAN}📊 FINAL STATISTICS:{Style.RESET_ALL}")
    print(f"  Successfully processed: {Fore.GREEN}{success_count}{Style.RESET_ALL} RAW files")
    print(f"  Errors: {Fore.RED}{error_count}{Style.RESET_ALL}")
    print(f"  Skipped: {skipped_count}")
    if filtered_count > 0:
        print(f"  Filtered: {filtered_count}")
    
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
        description='Mass RAW photo conversion to JPEG using RawTherapee CLI',
        epilog='Converts RAW image formats to high-quality JPEG using professional RawTherapee processing'
    )
    parser.add_argument(
        'file_list',
        help='Path to file containing list of image files to process'
    )
    parser.add_argument(
        '--suffix', '-s',
        default='_jpg',
        help='Suffix for converted files (default: _jpg)'
    )
    parser.add_argument(
        '--quality', '-q',
        type=int,
        default=95,
        help='JPEG quality (1-100, default: 95)'
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
    parser.add_argument(
        '--max-workers',
        type=int,
        default=4,
        help='Maximum number of parallel workers (default: 4)'
    )
    
    args = parser.parse_args()
    
    # Validate quality
    if not (1 <= args.quality <= 100):
        print(f"{Fore.RED}❌ JPEG quality must be between 1 and 100{Style.RESET_ALL}")
        return 1
    
    # Validate max workers
    if args.max_workers < 1:
        print(f"{Fore.RED}❌ Max workers must be at least 1{Style.RESET_ALL}")
        return 1
    elif args.max_workers > 8:
        print(f"{Fore.YELLOW}⚠️  Warning: Using more than 8 workers may not improve performance{Style.RESET_ALL}")
    
    # Check dependencies
    if not args.dry_run:
        success, message = check_rawtherapee_dependencies()
        if success:
            print(f"{Fore.GREEN}✅ {message}{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}❌ {message}{Style.RESET_ALL}")
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
            quality=args.quality,
            dry_run=args.dry_run,
            skip_existing=not args.no_skip_existing,
            pattern=args.pattern,
            max_workers=args.max_workers
        )
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}⚠️  Processing interrupted by user{Style.RESET_ALL}")
        if logger:
            logger.warning("Processing interrupted by user")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
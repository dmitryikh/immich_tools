#!/usr/bin/env python3
"""
Immich client.
Read all images from an album and assign date from filename if EXIF year matches target year.
Useful for fixing dates of photos/videos imported without proper metadata.
See parse_datetime_from_filename() for supported filename formats.

Usage:
python date_from_name.py --album "Album Name" --target-year 2025 --dry-run
"""

import os
import sys
import argparse
import re
from typing import Optional
from datetime import datetime, timezone
from dotenv import load_dotenv
from colorama import Fore, Style, init
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time
from tqdm import tqdm
from lib.immich import ImmichAPI

# Colorama init
init()

def parse_datetime_from_filename(filename: str) -> Optional[datetime]:
    """
    Parses datetime from filename in various formats
    
    Supported formats for images and videos:
    - '2018-03-10 21-30-06.JPG'
    - '2018-03-10_21-30-06.JPG'
    - '20180310_213006.JPG'
    - '2018-03-10T21:30:06.JPG'
    - '2021-01-05 12-22-19.MP4' (video files)
    - '2021-01-05_12-22-19.MOV'
    and other similar variants for different extensions
    """
    if not filename:
        return None
    
    # Patterns for various date formats in filenames
    # Supports images (JPG, JPEG, PNG, etc.) and videos (MP4, MOV, AVI, etc.)
    patterns = [
        # 2018-03-10 21-30-06.JPG / 2021-01-05 12-22-19.MP4
        r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2})-(\d{2})-(\d{2})\.(jpg|jpeg|png|gif|bmp|tiff|webp|mp4|mov|avi|mkv|wmv|flv|m4v|3gp)',
        # 2018-03-10_21-30-06.JPG / 2021-01-05_12-22-19.MP4
        r'(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})\.(jpg|jpeg|png|gif|bmp|tiff|webp|mp4|mov|avi|mkv|wmv|flv|m4v|3gp)',
        # 2018-03-10T21:30:06.JPG / 2021-01-05T12:22:19.MP4
        r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(jpg|jpeg|png|gif|bmp|tiff|webp|mp4|mov|avi|mkv|wmv|flv|m4v|3gp)',
        # 20180310_213006.JPG / 20210105_122219.MP4
        r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})\.(jpg|jpeg|png|gif|bmp|tiff|webp|mp4|mov|avi|mkv|wmv|flv|m4v|3gp)',
        # 2018-03-10 21:30:06.JPG / 2021-01-05 12:22:19.MP4
        r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\.(jpg|jpeg|png|gif|bmp|tiff|webp|mp4|mov|avi|mkv|wmv|flv|m4v|3gp)',
        # IMG_20180310_213006.JPG / VID_20210105_122219.MP4
        r'(IMG|VID)_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})\.(jpg|jpeg|png|gif|bmp|tiff|webp|mp4|mov|avi|mkv|wmv|flv|m4v|3gp)',
        
        # Fallback patterns without extension (for backward compatibility)
        # 2018-03-10 21-30-06
        r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2})-(\d{2})-(\d{2})',
        # 2018-03-10_21-30-06
        r'(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})',
        # 2018-03-10T21:30:06
        r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})',
        # 20180310_213006
        r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})',
        # 2018-03-10 21:30:06
        r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})',
        # IMG_20180310_213006 / VID_20210105_122219
        r'(IMG|VID)_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename, re.IGNORECASE)
        if match:
            try:
                groups = match.groups()
                
                # Determine group positions depending on pattern
                if groups[0] in ('IMG', 'VID'):
                    # Pattern like IMG_20180310_213006 or VID_20210105_122219
                    year = int(groups[1])
                    month = int(groups[2])
                    day = int(groups[3])
                    hour = int(groups[4])
                    minute = int(groups[5])
                    second = int(groups[6])
                else:
                    # Regular patterns
                    year = int(groups[0])
                    month = int(groups[1])
                    day = int(groups[2])
                    hour = int(groups[3])
                    minute = int(groups[4])
                    second = int(groups[5])
                
                # Create datetime object with UTC timezone
                dt = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
                return dt
            except (ValueError, IndexError) as e:
                print(f"{Fore.YELLOW}Date parsing error from '{filename}': {e}{Style.RESET_ALL}")
                continue
    
    print(f"{Fore.YELLOW}Could not parse date from filename: '{filename}'{Style.RESET_ALL}")
    return None

def process_asset_date_from_name(api: ImmichAPI, asset_id: str, target_year: int = 2025, dry_run: bool = False, verbose: bool = False) -> bool:
    """Processes single asset - extracts date from filename and updates metadata"""
    try:
        metadata = api.get_asset_metadata(asset_id)
        if not metadata:
            return False
            
        original_file_name = metadata.get('originalFileName', '')
        
        # Check creation year in EXIF data
        exif_info = metadata.get('exifInfo', {})
        date_time_original = exif_info.get('dateTimeOriginal')
        
        if date_time_original:
            try:
                # Parse year from dateTimeOriginal (format: '2025-10-17T19:34:56.9+00:00')
                exif_year = int(date_time_original[:4])
                
                # Process only files with specified creation year
                if exif_year != target_year:
                    if verbose:
                        print(f"{Fore.GRAY}Skipping {original_file_name} - EXIF year is {exif_year}, not {target_year}{Style.RESET_ALL}")
                    return False
                    
            except (ValueError, TypeError):
                if verbose:
                    print(f"{Fore.YELLOW}Warning: Cannot parse EXIF year from '{date_time_original}' in {original_file_name}{Style.RESET_ALL}")
                return False
        else:
            if verbose:
                print(f"{Fore.YELLOW}Skipping {original_file_name} - no EXIF dateTimeOriginal found{Style.RESET_ALL}")
            return False
        
        # Parse date from filename
        parsed_date = parse_datetime_from_filename(original_file_name)
        
        if not parsed_date:
            return False
        
        if verbose:
            print(f"{Fore.CYAN}Asset: {original_file_name}{Style.RESET_ALL}")
            print(f"  EXIF date:    {date_time_original} (year: {exif_year})")
            print(f"  Parsed date:  {parsed_date.isoformat()}")
        
        if dry_run:
            print(f"  {Fore.YELLOW}[DRY RUN] Would update date{Style.RESET_ALL}")
            return True
        else:
            # Update asset date
            success = api.update_asset_date(asset_id, parsed_date)
            if success:
                print(f"  {Fore.GREEN}✓ Date updated successfully{Style.RESET_ALL}")
            else:
                print(f"  {Fore.RED}✗ Failed to update date{Style.RESET_ALL}")
            return success
            
    except Exception as e:
        print(f"{Fore.RED}Asset processing error {asset_id}: {e}{Style.RESET_ALL}")
        return False

def main():
    """Main program function"""
    parser = argparse.ArgumentParser(
        description='Assigning photo date from filename in Immich'
    )
    parser.add_argument(
        '--album', 
        help='album name to process photos from'
    )
    parser.add_argument(
        '--server-url', 
        help='Immich server URL (overrides .env value)'
    )
    parser.add_argument(
        '--api-key', 
        help='Immich API key (overrides .env value)'
    )
    parser.add_argument(
        '--dry-run', 
        action='store_true', 
        help='Test mode: show found photos without updating dates'
    )
    parser.add_argument(
        '--workers', 
        type=int, 
        default=10,
        help='Parallel workers for processing assets (default: 10)'
    )
    parser.add_argument(
        '--target-year', 
        type=int, 
        default=2025,
        help='Only process files with EXIF year matching this value (default: 2025)'
    )
    parser.add_argument(
        '--verbose', 
        action='store_true',
        help='Enable verbose output with detailed processing information'
    )
    
    args = parser.parse_args()
    
    # Load configuration from .env file
    load_dotenv()
    
    server_url = args.server_url or os.getenv('IMMICH_SERVER_URL')
    api_key = args.api_key or os.getenv('IMMICH_API_KEY')
    
    if not server_url or not api_key:
        print(f"{Fore.RED}Error: IMMICH_SERVER_URL and IMMICH_API_KEY must be set{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}Create a .env file based on .env.example and fill in the settings{Style.RESET_ALL}")
        sys.exit(1)
    
    # Initialize API
    print(f"{Fore.BLUE}Connecting to Immich server: {server_url}{Style.RESET_ALL}")
    api = ImmichAPI(server_url, api_key)
    
    if not api.test_connection():
        print(f"{Fore.RED}Failed to connect to Immich server{Style.RESET_ALL}")
        sys.exit(1)

    print(f"{Fore.GREEN}Connected to Immich server successfully{Style.RESET_ALL}")

    # Get all images for processing
    print(f"{Fore.BLUE}Fetching image assets...{Style.RESET_ALL}")
    asset_ids = api.get_all_assets_from_album(args.album)
    print(f"{Fore.BLUE}Found {len(asset_ids)} images to process{Style.RESET_ALL}")

    if not asset_ids:
        print(f"{Fore.YELLOW}No assets found to process{Style.RESET_ALL}")
        sys.exit(0)
    
    # Statistics
    processed = 0
    updated = 0
    
    # Process assets with progress bar
    with tqdm(total=len(asset_ids), desc="Processing assets", unit="assets") as pbar:
        if args.workers > 1:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                future_to_asset_id = {
                    executor.submit(process_asset_date_from_name, api, asset_id, args.target_year, args.dry_run, args.verbose): asset_id 
                    for asset_id in asset_ids
                }
                
                for future in as_completed(future_to_asset_id):
                    result = future.result()
                    processed += 1
                    if result:
                        updated += 1
                    pbar.update(1)
        else:
            # Sequential processing
            for asset_id in asset_ids:
                result = process_asset_date_from_name(api, asset_id, args.target_year, args.dry_run, args.verbose)
                processed += 1
                if result:
                    updated += 1
                pbar.update(1)
    
    # Display statistics
    print(f"\n{Fore.GREEN}Processing completed!{Style.RESET_ALL}")
    print(f"Processed: {processed} assets")
    print(f"Updated: {updated} assets")
    
    if args.dry_run:
        print(f"{Fore.CYAN}This was a dry run - no changes were made{Style.RESET_ALL}")
        print(f"{Fore.BLUE}Run without --dry-run to actually update the dates{Style.RESET_ALL}")


if __name__ == "__main__":
    main()
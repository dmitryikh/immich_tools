#!/usr/bin/env python3
"""
Immich Photo Album Manager

Program for finding photos by resolution in Immich and assigning them to a new album.
Uses Immich REST API for working with media files.

Usage:
album_by_resolution.py  1179x2556 "Screenshots" --dry-run
"""

import os
import sys
import argparse
from typing import List, Optional, Tuple
from dotenv import load_dotenv
from colorama import Fore, Style, init
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time
from tqdm import tqdm
from lib.immich import ImmichAPI

# Colorama init
init()

class PhotoResolutionManager:
    """Class for managing photos by resolution"""
    
    def __init__(self, immich_api: ImmichAPI):
        self.api = immich_api
        self.matching_photos = []
        self.no_size = 0
        self.lock = Lock()
    
    def parse_resolution(self, resolution_str: str) -> Tuple[int, int]:
        """Parses resolution string in WIDTHxHEIGHT format"""
        try:
            parts = resolution_str.lower().split('x')
            if len(parts) != 2:
                raise ValueError("Invalid resolution format")
            return int(parts[0]), int(parts[1])
        except ValueError:
            raise ValueError(f"Invalid resolution format: {resolution_str}. Use WIDTHxHEIGHT format (e.g., 1920x1080)")
    
    def process_single_asset(self, asset_id: str, target_width: int, target_height: int, exact_match: bool) -> Optional[str]:
        """Processes single asset and checks its resolution"""
        try:
            # Get detailed asset information
            asset_info = self.api.get_asset_metadata(asset_id)
            
            exif_info = asset_info.get('exifInfo', {})
            
            width = exif_info.get('exifImageWidth')
            height = exif_info.get('exifImageHeight')
            
            if width is None or height is None:
                with self.lock:
                    self.no_size += 1
                return None

            # Check resolution
            if exact_match:
                if (width == target_width and height == target_height) or (height == target_width and width == target_height):
                    return asset_id
            else:
                # Non-strict comparison - allow small deviation
                if (abs(width - target_width) <= 10 and abs(height - target_height) <= 10) or (abs(height - target_width) <= 10 and abs(width - target_height) <= 10):
                    return asset_id
            
            return None
        except Exception as e:
            print(f"{Fore.YELLOW}Asset processing error {asset_id}: {e}{Style.RESET_ALL}")
            return None
    
    def find_photos_by_resolution(self, target_width: int, target_height: int, exact_match: bool = True, max_workers: int = 10) -> List[str]:
        print(f"{Fore.BLUE}Searching for photos with resolution {target_width}x{target_height}...{Style.RESET_ALL}")
        
        asset_ids = self.api.get_all_assets('IMAGE', limit=None)
        print(f"{Fore.BLUE}Found {len(asset_ids)} images.{Style.RESET_ALL}")

        if not asset_ids:
            print(f"{Fore.YELLOW}Assets not found{Style.RESET_ALL}")
            return []
        
        # Reset state
        self.matching_photos = []
        self.no_size = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            with tqdm(total=len(asset_ids), desc="Processing assets", unit="assets") as pbar:
                future_to_asset_id = {
                    executor.submit(self.process_single_asset, asset_id, target_width, target_height, exact_match): asset_id 
                    for asset_id in asset_ids
                }
                
                for future in as_completed(future_to_asset_id):
                    result = future.result()
                    if result is not None:
                        self.matching_photos.append(result)
                    
                    # Update progress bar
                    pbar.update(1)

        print(f"{Fore.GREEN}Found {len(self.matching_photos)} photos with matching resolution{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}Skipped {self.no_size} photos without resolution information{Style.RESET_ALL}")
        return self.matching_photos

    def create_album_with_photos(self, photo_ids: List[str], album_name: str, dry_run: bool = False) -> bool:
        if not photo_ids:
            print(f"{Fore.YELLOW}No photos to add to album{Style.RESET_ALL}")
            return False
        
        if dry_run:
            print(f"{Fore.CYAN}🔍 DRY RUN MODE: Photos will NOT be added to album{Style.RESET_ALL}")
            print(f"{Fore.BLUE}Album for addition: '{album_name}'{Style.RESET_ALL}")
            print(f"{Fore.GREEN}Number of photos to add: {len(photo_ids)}{Style.RESET_ALL}")
            return True
        
        print(f"{Fore.BLUE}Creating new album: {album_name}{Style.RESET_ALL}")
        album_id = self.api.create_album(album_name, asset_ids=photo_ids)
        if not album_id:
            return False

        print(f"{Fore.GREEN}Successfully added {len(photo_ids)} photos to album '{album_name}', id={album_id}{Style.RESET_ALL}")
        return True
        

def main():
    """Main program function"""
    parser = argparse.ArgumentParser(
        description='Search for photos by resolution in Immich and add them to album'
    )
    parser.add_argument(
        'resolution', 
        help='Resolution in WIDTHxHEIGHT format (e.g., 1920x1080)'
    )
    parser.add_argument(
        'album_name', 
        help='Album name for adding photos'
    )
    parser.add_argument(
        '--exact', 
        action='store_true', 
        help='Exact resolution match (default allows ±10 pixels deviation)'
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
        help='Test mode: show found photos without adding to album'
    )
    parser.add_argument(
        '--workers', 
        type=int, 
        default=10,
        help='Number of threads for parallel processing (default: 10)'
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
    
    # Parse resolution
    width, height = PhotoResolutionManager(None).parse_resolution(args.resolution)
    
    # Initialize API
    print(f"{Fore.BLUE}Connecting to Immich server: {server_url}{Style.RESET_ALL}")
    api = ImmichAPI(server_url, api_key)
    
    # Test connection
    if not api.test_connection():
        print(f"{Fore.RED}Failed to connect to Immich server{Style.RESET_ALL}")
        sys.exit(1)
    
    print(f"{Fore.GREEN}Server connection successful{Style.RESET_ALL}")
    
    # Initialize photo manager
    photo_manager = PhotoResolutionManager(api)

    # Search for photos
    photos = photo_manager.find_photos_by_resolution(width, height, args.exact, args.workers)
    
    if not photos:
        print(f"{Fore.YELLOW}No photos found with resolution {args.resolution}{Style.RESET_ALL}")
        sys.exit(0)
    
    print(f"{Fore.GREEN}Found {len(photos)} photos{Style.RESET_ALL}")
    
    # Show examples of found photos
    print(f"\n{Fore.CYAN}Examples of found photos (ID):{Style.RESET_ALL}")
    for i, photo_id in enumerate(photos[:5]):
        print(f"  {i+1}. Asset ID: {photo_id}")
    
    if len(photos) > 5:
        print(f"  ... and {len(photos) - 5} more photos")
    
    # In test mode don't require confirmation
    if args.dry_run:
        print(f"\n{Fore.CYAN}🔍 Test mode: showing results without changes{Style.RESET_ALL}")
    else:
        # User confirmation
        confirm = input(f"\n{Fore.YELLOW}Add all found photos to album '{args.album_name}'? (y/n): {Style.RESET_ALL}")
        if confirm.lower() not in ('y', 'yes'):
            print(f"{Fore.BLUE}Operation cancelled{Style.RESET_ALL}")
            sys.exit(0)
    
    # Add to album
    success = photo_manager.create_album_with_photos(photos, args.album_name, args.dry_run)
    
    if success:
        if args.dry_run:
            print(f"\n{Fore.GREEN}✓ Dry run completed successfully!{Style.RESET_ALL}")
        else:
            print(f"\n{Fore.GREEN}✓ Operation completed successfully!{Style.RESET_ALL}")
    else:
        if not args.dry_run:
            print(f"\n{Fore.RED}✗ Error occurred while adding photos to album{Style.RESET_ALL}")
            sys.exit(1)
            

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Media Analyzer

Program for recursive analysis of video and image files in directory.
Extracts metadata (codec, resolution, bitrate, duration for videos; 
EXIF data and resolution for images) using ffprobe and PIL/Pillow
and saves results to SQLite database.

Usage:
python video_analyzer.py test --database media_analysis.db --workers 16

python video_analyzer.py test --stats
"""

import os
import sys
import argparse
import sqlite3
import json
import subprocess
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from datetime import datetime
from colorama import Fore, Style, init
from tqdm import tqdm
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from PIL import Image
from PIL.ExifTags import TAGS

# Import from local library
from lib.metadata import get_image_metadata, get_video_metadata, VideoMetadataError, VideoCorruptedError, VideoTimeoutError, VideoNoStreamError
from lib.utils import VIDEO_EXTENSIONS, RAW_EXTENSIONS, IMAGE_EXTENSIONS, SUPPORTED_EXTENSIONS

# Initialize colorama with forced colors for container support
init(autoreset=True, strip=False)

class MediaAnalyzer:
    """Class for media file analysis (videos and images)"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db_lock = Lock()  # For thread-safe database operations
        self.init_database()
    
    def init_database(self):
        """Initializes SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS media_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT UNIQUE NOT NULL,
                file_name TEXT NOT NULL,
                file_size INTEGER,
                file_hash TEXT,
                media_type TEXT NOT NULL,  -- 'video' or 'image'
                creation_date TIMESTAMP,  -- from metadata if available
                duration REAL,  -- for videos only
                width INTEGER,
                height INTEGER,
                codec_name TEXT,  -- for videos
                codec_long_name TEXT,  -- for videos
                bit_rate INTEGER,  -- for videos
                frame_rate REAL,  -- for videos
                format_name TEXT,
                format_long_name TEXT,
                is_corrupted BOOLEAN DEFAULT 0,
                error_message TEXT,
                analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                modified_at TIMESTAMP
            )
        ''')
        
        # Indexes for fast searching
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_path ON media_files(file_path)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_media_type ON media_files(media_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_is_corrupted ON media_files(is_corrupted)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_codec ON media_files(codec_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_resolution ON media_files(width, height)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_creation_date ON media_files(creation_date)')
        
        conn.commit()
        conn.close()
    
    def get_file_hash(self, file_path: str) -> Optional[str]:
        """Calculates MD5 hash of file for uniqueness check"""
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                # Read file in chunks for large files
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            print(f"{Fore.YELLOW}Hash calculation error for {file_path}: {e}{Style.RESET_ALL}")
            return None
    
    def analyze_image_file(self, file_path: str) -> Dict:
        """Analyzes image file using PIL/Pillow and exiftool via Docker for metadata"""
        try:
            file_ext = Path(file_path).suffix.lower()
            
            # Initialize basic metadata structure
            metadata = {
                'is_corrupted': False,
                'media_type': 'image',
                'width': None,
                'height': None,
                'format_name': file_ext[1:] if file_ext else None,  # Remove dot
                'format_long_name': f"{file_ext[1:].upper()} Image" if file_ext else "Unknown Image",
                'creation_date': None
            }
            
            # For RAW files, we use exiftool for everything (including dimensions if available)
            if file_ext in RAW_EXTENSIONS:
                # Get creation date using exiftool via Docker
                exif_metadata = get_image_metadata(file_path)
                if 'creation_date' in exif_metadata:
                    metadata['creation_date'] = exif_metadata['creation_date']
                
                # Update format info for RAW files
                metadata['format_long_name'] = f"{file_ext[1:].upper()} RAW Image"
                
                return metadata
            
            # For non-RAW files, use PIL for dimensions and format, exiftool for creation date
            try:
                with Image.open(file_path) as img:
                    metadata.update({
                        'width': img.width,
                        'height': img.height,
                        'format_name': img.format.lower() if img.format else file_ext[1:],
                        'format_long_name': f"{img.format} Image" if img.format else f"{file_ext[1:].upper()} Image"
                    })
            except Exception as pil_error:
                # If PIL fails, we can still try to get metadata with exiftool
                metadata['error_message'] = f"PIL error (continuing with exiftool): {str(pil_error)}"
            
            # Get creation date using exiftool via Docker (works for both RAW and regular images)
            exif_metadata = get_image_metadata(file_path)
            if 'creation_date' in exif_metadata:
                metadata['creation_date'] = exif_metadata['creation_date']
            
            return metadata
                
        except Exception as e:
            return {
                'is_corrupted': True,
                'error_message': f"Image analysis error: {str(e)}",
                'media_type': 'image'
            }


    
    def is_file_processed(self, file_path: str, file_modified_time: float) -> bool:
        """Checks if file was already processed and hasn't changed"""
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT modified_at FROM media_files 
                WHERE file_path = ? AND modified_at >= ?
            ''', (file_path, file_modified_time))
            
            result = cursor.fetchone()
            conn.close()
            
            return result is not None
    
    def save_media_info(self, file_path: str, metadata: Dict):
        """Saves media file information (video or image) to database"""
        file_stats = os.stat(file_path)
        file_hash = self.get_file_hash(file_path) if not metadata.get('is_corrupted') else None
        
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO media_files (
                    file_path, file_name, file_size, file_hash, modified_at,
                    media_type, creation_date,
                    duration, width, height, codec_name, codec_long_name,
                    bit_rate, frame_rate, format_name, format_long_name,
                    is_corrupted, error_message, analyzed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_path,
                os.path.basename(file_path),
                file_stats.st_size,
                file_hash,
                file_stats.st_mtime,
                metadata.get('media_type'),
                metadata.get('creation_date'),
                metadata.get('duration'),
                metadata.get('width'),
                metadata.get('height'),
                metadata.get('codec_name'),
                metadata.get('codec_long_name'),
                metadata.get('bit_rate'),
                metadata.get('frame_rate'),
                metadata.get('format_name'),
                metadata.get('format_long_name'),
                metadata.get('is_corrupted', False),
                metadata.get('error_message'),
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
    
    def find_media_files(self, directory: str) -> List[str]:
        """Recursively finds all media files (videos and images) in directory"""
        media_files = []
        skipped_system_files = 0
        
        print(f"{Fore.BLUE}Searching for media files in {directory}...{Style.RESET_ALL}")
        
        for root, dirs, files in os.walk(directory):
            # Skip system directories
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            
            for file in files:
                # Skip system files (starting with dot)
                if file.startswith('.'):
                    skipped_system_files += 1
                    continue
                
                file_path = os.path.join(root, file)
                file_ext = Path(file).suffix.lower()
                
                if file_ext in SUPPORTED_EXTENSIONS:
                    media_files.append(file_path)
        
        if skipped_system_files > 0:
            print(f"{Fore.YELLOW}Skipped system files: {skipped_system_files}{Style.RESET_ALL}")
        
        return media_files
    
    def process_single_file(self, file_path: str, force_reanalyze: bool = False) -> Dict[str, any]:
        """Processes single media file (video or image) and returns result"""
        result = {
            'file_path': file_path,
            'processed': False,
            'skipped': False,
            'corrupted': False,
            'error': False,
            'error_message': None
        }
        
        try:
            # Check if file needs to be processed
            file_stats = os.stat(file_path)
            
            if not force_reanalyze and self.is_file_processed(file_path, file_stats.st_mtime):
                result['skipped'] = True
                return result
            
            # Determine file type and analyze accordingly
            file_ext = Path(file_path).suffix.lower()
            
            if file_ext in VIDEO_EXTENSIONS:
                try:
                    metadata = get_video_metadata(file_path)
                    # Add metadata that MediaAnalyzer expects
                    metadata['is_corrupted'] = False
                    metadata['media_type'] = 'video'
                    metadata['error_message'] = None
                except VideoCorruptedError as e:
                    metadata = {
                        'is_corrupted': True,
                        'error_message': str(e),
                        'media_type': 'video'
                    }
                except VideoTimeoutError as e:
                    metadata = {
                        'is_corrupted': True,
                        'error_message': str(e),
                        'media_type': 'video'
                    }
                except VideoNoStreamError as e:
                    metadata = {
                        'is_corrupted': True,
                        'error_message': str(e),
                        'media_type': 'video'
                    }
                except VideoMetadataError as e:
                    metadata = {
                        'is_corrupted': True,
                        'error_message': str(e),
                        'media_type': 'video'
                    }
            elif file_ext in IMAGE_EXTENSIONS:
                metadata = self.analyze_image_file(file_path)
            else:
                raise ValueError(f"Unsupported file type: {file_ext}")
            
            # Save to database
            self.save_media_info(file_path, metadata)
            
            result['processed'] = True
            if metadata.get('is_corrupted'):
                result['corrupted'] = True
                result['error_message'] = metadata.get('error_message')
            
        except Exception as e:
            result['error'] = True
            result['error_message'] = str(e)
            
            # Save error information
            error_metadata = {
                'is_corrupted': True,
                'error_message': f"Processing error: {str(e)}",
                'media_type': 'video' if Path(file_path).suffix.lower() in VIDEO_EXTENSIONS else 'image'
            }
            try:
                self.save_media_info(file_path, error_metadata)
            except:
                pass
        
        return result
    
    def analyze_directory(self, directory: str, force_reanalyze: bool = False, max_files: Optional[int] = None, max_workers: int = 4):
        """Analyzes all media files (videos and images) in directory"""
        if not os.path.exists(directory):
            print(f"{Fore.RED}Directory does not exist: {directory}{Style.RESET_ALL}")
            return
        
        # Find all media files
        media_files = self.find_media_files(directory)
        
        if not media_files:
            print(f"{Fore.YELLOW}No media files found in {directory}{Style.RESET_ALL}")
            return
        
        if max_files:
            media_files = media_files[:max_files]
        
        print(f"{Fore.GREEN}Found {len(media_files)} media files{Style.RESET_ALL}")
        print(f"{Fore.BLUE}Using {max_workers} threads for processing{Style.RESET_ALL}")
        
        # Statistics
        processed = 0
        skipped = 0
        corrupted = 0
        errors = 0
        
        # Parallel file processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            with tqdm(total=len(media_files), desc="Analyzing media files", unit="files") as pbar:
                # Submit all tasks to thread pool
                future_to_file = {
                    executor.submit(self.process_single_file, file_path, force_reanalyze): file_path 
                    for file_path in media_files
                }
                
                # Process completed tasks
                for future in as_completed(future_to_file):
                    file_path = future_to_file[future]
                    
                    try:
                        result = future.result()
                        
                        if result['processed']:
                            processed += 1
                            if result['corrupted']:
                                corrupted += 1
                                print(f"\n{Fore.RED}Corrupted file: {file_path} - {result['error_message']}{Style.RESET_ALL}")
                        elif result['skipped']:
                            skipped += 1
                        elif result['error']:
                            errors += 1
                            print(f"\n{Fore.RED}Processing error {file_path}: {result['error_message']}{Style.RESET_ALL}")
                        
                    except Exception as e:
                        errors += 1
                        print(f"\n{Fore.RED}Critical error processing {file_path}: {e}{Style.RESET_ALL}")
                    
                    # Update progress bar
                    pbar.set_postfix(
                        processed=processed, 
                        skipped=skipped, 
                        corrupted=corrupted, 
                        errors=errors
                    )
                    pbar.update(1)
        
        # Output final statistics
        print(f"\n{Fore.GREEN}Analysis completed!{Style.RESET_ALL}")
        print(f"Processed: {processed}")
        print(f"Skipped (already processed): {skipped}")
        print(f"Corrupted files: {corrupted}")
        print(f"Errors: {errors}")
    
    def get_statistics(self) -> Dict:
        """Gets statistics from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # General statistics
        cursor.execute('SELECT COUNT(*) FROM media_files')
        total_files = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM media_files WHERE media_type = "video"')
        video_files = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM media_files WHERE media_type = "image"')
        image_files = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM media_files WHERE is_corrupted = 1')
        corrupted_files = cursor.fetchone()[0]
        
        cursor.execute('SELECT SUM(file_size) FROM media_files WHERE is_corrupted = 0')
        total_size = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT SUM(duration) FROM media_files WHERE is_corrupted = 0 AND media_type = "video"')
        total_duration = cursor.fetchone()[0] or 0
        
        # Top codecs (videos only)
        cursor.execute('''
            SELECT codec_name, COUNT(*) as count 
            FROM media_files 
            WHERE is_corrupted = 0 AND media_type = "video" AND codec_name IS NOT NULL
            GROUP BY codec_name 
            ORDER BY count DESC 
            LIMIT 10
        ''')
        top_codecs = cursor.fetchall()
        
        # Top resolutions (both videos and images)
        cursor.execute('''
            SELECT width || 'x' || height as resolution, COUNT(*) as count 
            FROM media_files 
            WHERE is_corrupted = 0 AND width > 0 AND height > 0
            GROUP BY width, height 
            ORDER BY count DESC 
            LIMIT 10
        ''')
        top_resolutions = cursor.fetchall()
        
        conn.close()
        
        return {
            'total_files': total_files,
            'video_files': video_files,
            'image_files': image_files,
            'corrupted_files': corrupted_files,
            'valid_files': total_files - corrupted_files,
            'total_size_gb': total_size / (1024**3),
            'total_duration_hours': total_duration / 3600,
            'top_codecs': top_codecs,
            'top_resolutions': top_resolutions
        }

def main():
    """Main program function"""
    parser = argparse.ArgumentParser(
        description='Media file analysis (videos and images) using ffprobe and PIL, saving to SQLite'
    )
    parser.add_argument(
        'directory',
        help='Directory for recursive media file search (videos and images)'
    )
    parser.add_argument(
        '--database', '-d',
        default='video_analysis.db',
        help='Path to SQLite database (default: video_analysis.db)'
    )
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Force reanalysis of all files'
    )
    parser.add_argument(
        '--max-files',
        type=int,
        help='Maximum number of files to process (for testing)'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show statistics from database'
    )
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=4,
        help='Number of threads for parallel processing (default: 4)'
    )
    
    args = parser.parse_args()
    
    # Initialize analyzer
    analyzer = MediaAnalyzer(args.database)
    
    if args.stats:
        # Show statistics
        stats = analyzer.get_statistics()
        
        print(f"\n{Fore.CYAN}📊 Media file statistics{Style.RESET_ALL}")
        print(f"{'='*50}")
        print(f"Total files: {stats['total_files']}")
        print(f"  Videos: {stats['video_files']}")
        print(f"  Images: {stats['image_files']}")
        print(f"Valid files: {stats['valid_files']}")
        print(f"Corrupted files: {stats['corrupted_files']}")
        print(f"Total size: {stats['total_size_gb']:.2f} GB")
        print(f"Video duration: {stats['total_duration_hours']:.2f} hours")
        
        if stats['top_codecs']:
            print(f"\n{Fore.YELLOW}🎥 Top video codecs:{Style.RESET_ALL}")
            for codec, count in stats['top_codecs']:
                print(f"  {codec}: {count} files")
        
        if stats['top_resolutions']:
            print(f"\n{Fore.YELLOW}📐 Top resolutions:{Style.RESET_ALL}")
            for resolution, count in stats['top_resolutions']:
                print(f"  {resolution}: {count} files")
    
    else:
        # Analyze directory
        analyzer.analyze_directory(
            args.directory,
            force_reanalyze=args.force,
            max_files=args.max_files,
            max_workers=args.workers
        )

if __name__ == "__main__":
    main()
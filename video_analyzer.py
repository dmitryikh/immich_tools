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

# Initialize colorama
init()

class MediaAnalyzer:
    """Class for media file analysis (videos and images)"""
    
    # Supported video formats
    VIDEO_EXTENSIONS = {
        '.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', 
        '.m4v', '.3gp', '.ogv', '.f4v', '.asf', '.rm', '.rmvb',
        '.vob', '.ts', '.mts', '.m2ts', '.mpg', '.mpeg', '.m2v'
    }
    
    # Supported image formats
    IMAGE_EXTENSIONS = {
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', 
        '.webp', '.heic', '.heif', '.raw', '.cr2', '.nef', '.arw', '.rw2'
    }
    
    # All supported formats
    SUPPORTED_EXTENSIONS = VIDEO_EXTENSIONS | IMAGE_EXTENSIONS
    
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
                camera_make TEXT,  -- for images (EXIF)
                camera_model TEXT,  -- for images (EXIF)
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
        """Analyzes image file using PIL/Pillow"""
        try:
            # Check if file is RAW format that PIL cannot handle
            file_ext = Path(file_path).suffix.lower()
            if file_ext in ['.rw2', '.raw', '.cr2', '.nef', '.arw']:
                # For RAW files, we can only get basic file info
                return {
                    'is_corrupted': False,
                    'media_type': 'image',
                    'width': None,
                    'height': None,
                    'format_name': file_ext[1:],  # Remove dot
                    'format_long_name': f"{file_ext[1:].upper()} RAW Image",
                    'creation_date': None,
                    'camera_make': None,
                    'camera_model': None
                }
            
            with Image.open(file_path) as img:
                # Basic image info
                metadata = {
                    'is_corrupted': False,
                    'media_type': 'image',
                    'width': img.width,
                    'height': img.height,
                    'format_name': img.format.lower() if img.format else None,
                    'format_long_name': f"{img.format} Image" if img.format else "Unknown Image",
                    'creation_date': None,
                    'camera_make': None,
                    'camera_model': None
                }
                
                # Extract EXIF data if available - use different methods for different formats
                exif_data = None
                try:
                    # Try the standard method first
                    if hasattr(img, '_getexif'):
                        exif_data = img._getexif()
                    else:
                        # For TIFF and other formats, try getexif() method
                        exif_data = img.getexif()
                except (AttributeError, OSError):
                    # If both methods fail, continue without EXIF
                    pass
                
                if exif_data:
                    # Parse EXIF data - handle both old dict format and new ExifTags format
                    exif_dict = {}
                    try:
                        if isinstance(exif_data, dict):
                            # Old format: already a dictionary
                            for tag_id, value in exif_data.items():
                                tag = TAGS.get(tag_id, tag_id)
                                exif_dict[tag] = value
                        else:
                            # New format: ExifTags object
                            for tag_id, value in exif_data.items():
                                tag = TAGS.get(tag_id, tag_id)
                                exif_dict[tag] = value
                    except Exception:
                        # If EXIF parsing fails, continue without it
                        pass
                    
                    if exif_dict:
                        # Extract creation date
                        for date_tag in ['DateTimeOriginal', 'DateTime', 'DateTimeDigitized']:
                            if date_tag in exif_dict:
                                try:
                                    date_str = exif_dict[date_tag]
                                    if isinstance(date_str, str) and date_str.strip():
                                        # Parse EXIF datetime format: "YYYY:MM:DD HH:MM:SS"
                                        creation_date = datetime.strptime(date_str.strip(), '%Y:%m:%d %H:%M:%S')
                                        metadata['creation_date'] = creation_date.isoformat()
                                        break
                                except (ValueError, TypeError):
                                    continue
                        
                        # Extract camera info
                        make = exif_dict.get('Make')
                        model = exif_dict.get('Model')
                        
                        if make and isinstance(make, str):
                            metadata['camera_make'] = make.strip()
                        if model and isinstance(model, str):
                            metadata['camera_model'] = model.strip()
                
                return metadata
                
        except Exception as e:
            return {
                'is_corrupted': True,
                'error_message': f"Image analysis error: {str(e)}",
                'media_type': 'image'
            }

    def analyze_video_with_ffprobe(self, file_path: str) -> Dict:
        """Analyzes video file using ffprobe"""
        try:
            # ffprobe command to get video information
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                '-show_streams',
                '-select_streams', 'v:0',  # Only first video stream
                file_path
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30  # 30 second timeout
            )
            
            if result.returncode != 0:
                return {
                    'is_corrupted': True,
                    'error_message': f"ffprobe error: {result.stderr.strip()}",
                    'media_type': 'video'
                }
            
            data = json.loads(result.stdout)
            
            # Extract format information
            format_info = data.get('format', {})
            
            # Find first video stream
            video_stream = None
            for stream in data.get('streams', []):
                if stream.get('codec_type') == 'video':
                    video_stream = stream
                    break
            
            if not video_stream:
                return {
                    'is_corrupted': True,
                    'error_message': "No video stream found",
                    'media_type': 'video'
                }
            
            # Extract metadata
            metadata = {
                'is_corrupted': False,
                'media_type': 'video',
                'duration': float(format_info.get('duration', 0)),
                'width': int(video_stream.get('width', 0)),
                'height': int(video_stream.get('height', 0)),
                'codec_name': video_stream.get('codec_name', ''),
                'codec_long_name': video_stream.get('codec_long_name', ''),
                'bit_rate': int(format_info.get('bit_rate', 0)),
                'format_name': format_info.get('format_name', ''),
                'format_long_name': format_info.get('format_long_name', ''),
                'frame_rate': 0.0,
                'creation_date': None,
                'error_message': None
            }
            
            # Extract creation date from format tags if available
            tags = format_info.get('tags', {})
            for tag_key in ['creation_time', 'date', 'DATE']:
                if tag_key in tags:
                    try:
                        # Parse ISO format datetime
                        date_str = tags[tag_key]
                        if date_str and date_str != '0000-00-00T00:00:00.000000Z':
                            # Remove microseconds and timezone for parsing
                            clean_date = date_str.replace('Z', '').split('.')[0]
                            if 'T' in clean_date:
                                creation_date = datetime.fromisoformat(clean_date)
                                metadata['creation_date'] = creation_date.isoformat()
                                break
                    except (ValueError, TypeError):
                        continue
            
            # Calculate frame rate
            r_frame_rate = video_stream.get('r_frame_rate', '0/1')
            if '/' in r_frame_rate:
                num, den = r_frame_rate.split('/')
                if int(den) != 0:
                    metadata['frame_rate'] = float(num) / float(den)
            
            return metadata
            
        except subprocess.TimeoutExpired:
            return {
                'is_corrupted': True,
                'error_message': "ffprobe timeout (30s)",
                'media_type': 'video'
            }
        except json.JSONDecodeError as e:
            return {
                'is_corrupted': True,
                'error_message': f"JSON decode error: {str(e)}",
                'media_type': 'video'
            }
        except Exception as e:
            return {
                'is_corrupted': True,
                'error_message': f"Analysis error: {str(e)}",
                'media_type': 'video'
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
                    camera_make, camera_model,
                    is_corrupted, error_message, analyzed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                metadata.get('camera_make'),
                metadata.get('camera_model'),
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
                
                if file_ext in self.SUPPORTED_EXTENSIONS:
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
            
            if file_ext in self.VIDEO_EXTENSIONS:
                metadata = self.analyze_video_with_ffprobe(file_path)
            elif file_ext in self.IMAGE_EXTENSIONS:
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
                'media_type': 'video' if Path(file_path).suffix.lower() in self.VIDEO_EXTENSIONS else 'image'
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
        
        # Top cameras (images only)
        cursor.execute('''
            SELECT camera_make || ' ' || camera_model as camera, COUNT(*) as count 
            FROM media_files 
            WHERE is_corrupted = 0 AND media_type = "image" AND camera_make IS NOT NULL AND camera_model IS NOT NULL
            GROUP BY camera_make, camera_model 
            ORDER BY count DESC 
            LIMIT 10
        ''')
        top_cameras = cursor.fetchall()
        
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
            'top_resolutions': top_resolutions,
            'top_cameras': top_cameras
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
    
    # Check for ffprobe availability
    try:
        subprocess.run(['ffprobe', '-version'], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print(f"{Fore.RED}Error: ffprobe not found. Install FFmpeg.{Style.RESET_ALL}")
        print("Install on macOS: brew install ffmpeg")
        print("Install on Ubuntu: sudo apt install ffmpeg")
        sys.exit(1)
    
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
        
        if stats['top_cameras']:
            print(f"\n{Fore.YELLOW}📷 Top cameras:{Style.RESET_ALL}")
            for camera, count in stats['top_cameras']:
                print(f"  {camera}: {count} files")
    
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
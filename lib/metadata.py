#!/usr/bin/env python3
"""
Metadata manipulation functions for image and video files

Functions for setting creation time metadata using Docker-based tools.
"""

import os
import subprocess
import json
from datetime import datetime
from pathlib import Path


class VideoMetadataError(Exception):
    """Base exception for video metadata operations"""
    pass


class VideoCorruptedError(VideoMetadataError):
    """Exception raised when video file is corrupted or unreadable"""
    pass


class VideoTimeoutError(VideoMetadataError):
    """Exception raised when ffprobe operation times out"""
    pass


class VideoNoStreamError(VideoMetadataError):
    """Exception raised when no video stream is found in the file"""
    pass


def set_image_exif_datetime(file_path: str, creation_time: datetime, dry_run: bool = False) -> bool:
    """
    Set EXIF datetime for image files using exiftool via Docker
    
    Args:
        file_path: Path to the image file
        creation_time: DateTime to set as creation time
        dry_run: If True, don't actually modify the file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if dry_run:
            return True
            
        # Format datetime for exiftool (YYYY:MM:DD HH:MM:SS)
        time_str = creation_time.strftime('%Y:%m:%d %H:%M:%S')
        
        # Get directory and filename
        file_path = os.path.abspath(file_path)
        input_dir = os.path.dirname(file_path)
        filename = os.path.basename(file_path)
        
        # Container paths
        container_file = f'/data/{filename}'
        
        # Use exiftool directly to set EXIF datetime tags
        cmd = [
            'exiftool', '-overwrite_original',
            f'-DateTimeOriginal={time_str}',
            f'-DateTimeDigitized={time_str}',
            f'-DateTime={time_str}',
            '-P',  # preserve file timestamps
            file_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return result.returncode == 0
        
    except Exception:
        return False


def set_video_metadata_datetime(file_path: str, creation_time: datetime, dry_run: bool = False) -> bool:
    """
    Set creation time metadata for video files using ffmpeg via Docker
    
    Args:
        file_path: Path to the video file
        creation_time: DateTime to set as creation time
        dry_run: If True, don't actually modify the file
        
    Returns:
        bool: True if successful, False otherwise
        
    Note:
        Only works with formats that support metadata: MP4, MOV, AVI, MKV, WebM
        Legacy formats like MPEG-1/MPEG-2 (.mpg, .mpeg) don't support creation_time metadata
    """
    try:
        if dry_run:
            return True
            
        # Check if format supports metadata
        file_ext = Path(file_path).suffix.lower()
        unsupported_formats = {'.mpg', '.mpeg', '.m2v', '.vob', '.dat'}
        
        if file_ext in unsupported_formats:
            # Legacy MPEG formats don't support creation_time metadata
            return False
            
        # Format datetime for ffmpeg (ISO 8601 format)
        time_str = creation_time.strftime('%Y-%m-%dT%H:%M:%S')
        
        # Get absolute path and create temp file name
        file_path = os.path.abspath(file_path)
        file_ext = Path(file_path).suffix
        temp_file = f'{file_path}_temp{file_ext}'
        
        # Use ffmpeg directly to set metadata without re-encoding
        cmd = [
            'sh', '-c',
            f'ffmpeg -i "{file_path}" -c copy '
            f'-map_metadata 0 '
            f'-metadata "creation_time={time_str}" '
            f'-y "{temp_file}" && '
            f'touch -r "{file_path}" "{temp_file}" && '
            f'mv "{temp_file}" "{file_path}"'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        print(result.stderr)
        return result.returncode == 0
        
    except Exception:
        return False


def get_image_metadata(file_path: str) -> dict:
    """
    Get image metadata including creation date using exiftool via Docker
    
    Args:
        file_path: Path to the image file
        
    Returns:
        dict: Dictionary with creation_date (ISO format) if found, empty dict otherwise
    """
    try:
        # Get absolute file path
        file_path = os.path.abspath(file_path)
        
        # Use exiftool directly to get comprehensive metadata
        cmd = [
            'exiftool', '-json', '-DateTimeOriginal', '-CreateDate', '-CreationDate',
            file_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            return {}
            
        # Parse JSON output
        try:
            data = json.loads(result.stdout)
            if isinstance(data, list) and len(data) > 0:
                metadata = data[0]
                
                # Try to find creation date in priority order
                datetime_fields = ['DateTimeOriginal', 'CreateDate', 'CreationDate']
                for field in datetime_fields:
                    if field in metadata and metadata[field]:
                        date_str = metadata[field]
                        if isinstance(date_str, str) and date_str.strip():
                            try:
                                # Parse EXIF datetime format: "YYYY:MM:DD HH:MM:SS"
                                if ':' in date_str and len(date_str) >= 19:
                                    creation_date = datetime.strptime(date_str[:19], '%Y:%m:%d %H:%M:%S')
                                    return {'creation_date': creation_date.isoformat()}
                            except (ValueError, TypeError):
                                continue
                                
        except (json.JSONDecodeError, IndexError, KeyError):
            pass
            
        return {}
        
    except Exception:
        return {}


def get_video_metadata(file_path: str) -> dict:
    """
    Get video metadata using ffprobe via Docker
    
    Args:
        file_path: Path to the video file
        
    Returns:
        dict: Dictionary with video metadata including creation_date, dimensions, codec info, etc.
    
    Raises:
        VideoCorruptedError: If the video file is corrupted or ffprobe fails
        VideoTimeoutError: If ffprobe operation times out
        VideoNoStreamError: If no video stream is found in the file
        VideoMetadataError: For other video metadata related errors
    """
    # Get absolute file path
    file_path = os.path.abspath(file_path)
    
    # Use ffprobe directly to get video information
    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json',
        '-show_format', '-show_streams', '-select_streams', 'v:0',
        file_path
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            error_msg = result.stderr.strip() if result.stderr else "ffprobe returned non-zero exit code"
            raise VideoCorruptedError(f"ffprobe error: {error_msg}")
            
        data = json.loads(result.stdout)
        
    except subprocess.TimeoutExpired:
        raise VideoTimeoutError("ffprobe timeout (30s)")
    except json.JSONDecodeError as e:
        raise VideoCorruptedError(f"JSON decode error: {str(e)}")
    except subprocess.SubprocessError as e:
        raise VideoMetadataError(f"Subprocess error: {str(e)}")
    
    # Extract format information
    format_info = data.get('format', {})
    
    # Find first video stream
    video_stream = None
    for stream in data.get('streams', []):
        if stream.get('codec_type') == 'video':
            video_stream = stream
            break
    
    if not video_stream:
        raise VideoNoStreamError("No video stream found in file")
    
    # Extract metadata
    metadata = {
        'duration': float(format_info.get('duration', 0)),
        'width': int(video_stream.get('width', 0)),
        'height': int(video_stream.get('height', 0)),
        'codec_name': video_stream.get('codec_name', ''),
        'codec_long_name': video_stream.get('codec_long_name', ''),
        'bit_rate': int(format_info.get('bit_rate', 0)),
        'format_name': format_info.get('format_name', ''),
        'format_long_name': format_info.get('format_long_name', ''),
        'frame_rate': 0.0,
        'creation_date': None
    }
    
    # Extract creation date from format tags if available
    tags = format_info.get('tags', {})
    for tag_key in ['creation_time']:
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
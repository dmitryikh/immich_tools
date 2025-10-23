#!/usr/bin/env python3
"""
Video conversion functions using FFmpeg via Docker

Functions for building FFmpeg commands and handling video conversion operations.
"""

import os
import subprocess
import shutil
import tempfile
import time
import logging
from pathlib import Path


def build_ffmpeg_command(input_path, output_path):
    """Builds FFmpeg command for encoding directly (no Docker)"""
    # Get absolute paths
    input_abs = os.path.abspath(input_path)
    output_abs = os.path.abspath(output_path)

    # Build direct ffmpeg command
    cmd = [
        'ffmpeg',
        '-hide_banner',
        '-i', input_abs,
        '-vf', 'scale=\'min(1280,iw)\':-2,format=yuv420p',
        '-c:v', 'libx264',
        '-preset', 'fast',
        '-crf', '22',
        '-profile:v', 'high',
        '-c:a', 'aac',
        '-b:a', '160k',
        '-ac', '2',
        '-ar', '48000',
        '-movflags', '+faststart',
        '-map_metadata', '0',
        '-y',  # Overwrite output file
        output_abs
    ]
    
    return cmd


def preserve_file_timestamp(source_path, destination_path):
    """
    Preserves the modification time (mtime) of the source file to the destination file
    
    Args:
        source_path: Path to the source file
        destination_path: Path to the destination file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Use touch -r command to preserve timestamp
        result = subprocess.run([
            'touch', '-r', source_path, destination_path
        ], capture_output=True, text=True, timeout=10)
        
        return result.returncode == 0
        
    except (subprocess.TimeoutExpired, FileNotFoundError):
        # Fallback to Python's os.utime if touch command fails
        try:
            source_stat = os.stat(source_path)
            os.utime(destination_path, (source_stat.st_atime, source_stat.st_mtime))
            return True
        except OSError:
            return False


def get_output_path(original_path, suffix="_encoded"):
    """Generates output file path (adds suffix and changes extension to .mp4)"""
    path_obj = Path(original_path)
    # Create new name with suffix
    new_name = f"{path_obj.stem}{suffix}.mp4"
    output_path = path_obj.parent / new_name
    return str(output_path)


def encode_video_file(input_path: str, output_path: str, dry_run: bool = False) -> dict:
    """
    Encode single video file using FFmpeg via Docker with atomic write
    
    Args:
        input_path: Path to the input video file
        output_path: Path to the output video file
        dry_run: If True, don't actually encode the file
        
    Returns:
        dict: Result dictionary with success status, file sizes, duration, error info
    """
    result = {
        'input_path': input_path,
        'output_path': output_path,
        'success': False,
        'error': None,
        'original_size': 0,
        'output_size': 0,
        'duration': 0
    }
    
    try:
        # Get original file size
        if os.path.exists(input_path):
            result['original_size'] = os.path.getsize(input_path)
        
        if dry_run:
            result['success'] = True
            result['output_size'] = result['original_size'] * 0.6  # Estimated compression
            return result
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)
        
        # Create temporary file in the same directory as final file
        temp_fd, temp_path = tempfile.mkstemp(
            suffix='.mp4',
            dir=output_dir,
            prefix=f"{Path(output_path).stem}_"
        )
        os.close(temp_fd)  # Close file descriptor
        
        try:
            # Build FFmpeg command with temporary file
            cmd = build_ffmpeg_command(input_path, temp_path)
            
            # Start encoding
            start_time = time.time()
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # Maximum 1 hour per file
            )
            
            result['duration'] = time.time() - start_time
            
            if process.returncode == 0:
                if os.path.exists(temp_path):
                    # Atomically move temporary file to final location
                    temp_size = os.path.getsize(temp_path)
                    shutil.move(temp_path, output_path)
                    result['output_size'] = os.path.getsize(output_path)
                    
                    # Preserve original file timestamp
                    preserve_file_timestamp(input_path, output_path)
                    
                    result['success'] = True
                else:
                    result['error'] = "Temporary file not created"
            else:
                result['error'] = f"FFmpeg error: {process.stderr}"
        
        finally:
            # Clean up temporary file if it remains
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass  # Ignore deletion errors
            
    except subprocess.TimeoutExpired:
        result['error'] = "Encoding timeout"
    except Exception as e:
        result['error'] = str(e)
    
    return result
#!/usr/bin/env python3
"""
RAW image conversion library using RawTherapee CLI
Handles professional RAW to JPEG conversion with metadata preservation
"""

import os
import subprocess
import tempfile
from pathlib import Path
from PIL import Image
from .utils import RAW_EXTENSIONS

def is_raw_file(file_path):
    """Checks if file is a RAW format"""
    return Path(file_path).suffix.lower() in RAW_EXTENSIONS

def convert_raw_image_rawtherapee(input_path, temp_output_path, quality=95, logger=None):
    """Converts RAW image to JPEG using RawTherapee CLI"""
    try:
        # Prepare absolute paths
        input_abs = os.path.abspath(input_path)
        temp_abs = os.path.abspath(temp_output_path)
        
        cmd = [
            'rawtherapee-cli',
            '-d',  # Don't save sidecar files
            '-s',  # Suppress stdout progress output
            '-n',  # Don't overwrite existing output files
            '-t',  # Use multithreading
            '-o', temp_abs,  # Output file path
            f'-j{quality}',  # JPEG quality
            '-Y',  # Overwrite output file if it exists
            '-c', input_abs  # Input file path
        ]
        
        # Run RawTherapee CLI (suppress output for parallel processing)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode != 0:
            raise Exception(f"RawTherapee CLI failed: {result.stderr}")
        
        # Check if output file was created
        if not os.path.exists(temp_output_path):
            raise Exception("RawTherapee CLI did not create output file")
        
        # Inherit modification time from original RAW file using touch -r
        # This preserves the original file timestamps for proper chronological sorting
        touch_cmd = ['touch', '-r', input_abs, temp_abs]
        touch_result = subprocess.run(touch_cmd, capture_output=True, text=True, timeout=10)
        
        # Load the converted image to get dimensions (RawTherapee handles all metadata automatically)
        with Image.open(temp_output_path) as img:
            image_info = {
                'width': img.width,
                'height': img.height,
                'mode': img.mode
            }
        
        return image_info
        
    except subprocess.TimeoutExpired:
        raise Exception("RawTherapee CLI timeout")
    except Exception as e:
        raise Exception(f"RAW processing with RawTherapee failed: {str(e)}")

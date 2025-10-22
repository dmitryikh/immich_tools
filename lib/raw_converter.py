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
    """Converts RAW image to JPEG using RawTherapee CLI in Docker"""
    try:
        # Prepare paths for Docker
        input_abs = os.path.abspath(input_path)
        temp_abs = os.path.abspath(temp_output_path)
        
        # Get directories
        input_dir = os.path.dirname(input_abs)
        output_dir = os.path.dirname(temp_abs)
        
        # Get just filenames for container paths
        input_filename = os.path.basename(input_abs)
        output_filename = os.path.basename(temp_abs)
        
        # If input and output are in same directory, mount one directory
        if input_dir == output_dir:
            container_input = f"/data/{input_filename}"
            container_output = f"/data/{output_filename}"
            
            # Build RawTherapee CLI command
            cmd = [
                'docker', 'run', '--rm',
                '-v', f'{input_dir}:/data',
                'immich_tools',
                f'rawtherapee-cli -d -s -n -t -o {container_output} -j{quality} -Y -c {container_input}'
            ]
        else:
            # Mount separate input/output directories
            container_input = f"/input/{input_filename}"
            container_output = f"/output/{output_filename}"
            
            cmd = [
                'docker', 'run', '--rm',
                '-v', f'{input_dir}:/input:ro',
                '-v', f'{output_dir}:/output',
                'immich_tools',
                f'rawtherapee-cli -d -s -n -t -o {container_output} -j{quality} -Y -c {container_input}'
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

def check_rawtherapee_dependencies():
    """Checks if RawTherapee CLI Docker dependencies are available"""
    try:
        # Check Docker availability
        result = subprocess.run(['docker', '--version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            return False, "Docker not available"
        
        # Check immich_tools Docker image
        result = subprocess.run([
            'docker', 'run', '--rm', 'immich_tools', 'rawtherapee-cli', '--help'
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            return True, "RawTherapee CLI Docker image ready"
        else:
            return False, "RawTherapee CLI not available in immich_tools Docker image"
            
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False, "Docker or RawTherapee CLI not available"
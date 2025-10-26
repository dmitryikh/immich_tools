#!/usr/bin/env python3
"""
Setup test data structure for immich_tools testing.

Creates a test directory structure with various types of media files:
- RAW photos (DNG format)
- JPEG photos with metadata
- Photos without creation metadata
- Modern video (H264/MP4)
- Legacy video format (AVI/MJPEG)
- Corrupted video file
- Corrupted photo file
- System files (.txt, .pdf)

Usage:
    python setup_test_data.py [--output-dir DIR] [--cleanup]
    
    --output-dir: Directory to create test structure (default: test_data)
    --cleanup: Remove existing test data before creating new
"""

import os
import sys
import shutil
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

def set_file_mtime(file_path, target_datetime):
    """
    Set the modification time (mtime) of a file to match target datetime.
    
    Args:
        file_path: Path to the file
        target_datetime: datetime object to set as mtime
    """
    try:
        timestamp = target_datetime.timestamp()
        os.utime(file_path, (timestamp, timestamp))  # (atime, mtime)
        return True
    except Exception as e:
        print(f"{Fore.YELLOW}Warning: Could not set mtime for {file_path}: {e}{Style.RESET_ALL}")
        return False

def create_minimal_jpeg_with_exif(output_path, creation_date=None):
    """
    Create a minimal JPEG file with EXIF metadata using Python PIL.
    """
    try:
        from PIL import Image
        import piexif
        
        # Create a simple black image
        img = Image.new('RGB', (100, 100), color='black')
        
        if creation_date:
            # Format date for EXIF
            date_str = creation_date.strftime('%Y:%m:%d %H:%M:%S')
            
            # Create EXIF data
            exif_dict = {
                "0th": {
                    piexif.ImageIFD.Make: "Test Camera",
                    piexif.ImageIFD.Model: "Test Model",
                    piexif.ImageIFD.Software: "immich_tools_test",
                    piexif.ImageIFD.DateTime: date_str,
                },
                "Exif": {
                    piexif.ExifIFD.DateTimeOriginal: date_str,
                    piexif.ExifIFD.DateTimeDigitized: date_str,
                    piexif.ExifIFD.ColorSpace: 1,  # sRGB
                },
                "GPS": {},
                "1st": {},
                "thumbnail": None
            }
            
            # Convert to bytes
            exif_bytes = piexif.dump(exif_dict)
            img.save(output_path, format='JPEG', exif=exif_bytes, quality=95)
        else:
            # Save without EXIF
            img.save(output_path, format='JPEG', quality=95)
            
        return True
        
    except ImportError:
        # PIL/piexif not available, fallback to simple JPEG
        return create_simple_jpeg(output_path)
    except Exception as e:
        # Any other error, fallback to simple JPEG
        return create_simple_jpeg(output_path)


def create_simple_jpeg(output_path):
    """
    Create a minimal JPEG file without dependencies.
    Uses a minimal JPEG header for a 1x1 black pixel.
    """
    # Minimal JPEG file (1x1 black pixel)
    jpeg_data = bytes([
        0xFF, 0xD8,  # SOI marker
        0xFF, 0xE0,  # APP0 marker
        0x00, 0x10,  # APP0 length
        0x4A, 0x46, 0x49, 0x46, 0x00,  # JFIF identifier
        0x01, 0x01,  # JFIF version
        0x01,  # Aspect ratio units
        0x00, 0x01, 0x00, 0x01,  # X and Y density
        0x00, 0x00,  # Thumbnail width and height
        0xFF, 0xC0,  # SOF0 marker
        0x00, 0x11,  # SOF0 length
        0x08,  # Data precision
        0x00, 0x01, 0x00, 0x01,  # Height and width (1x1)
        0x01,  # Number of components
        0x01, 0x11, 0x00,  # Component info
        0xFF, 0xC4,  # DHT marker
        0x00, 0x14,  # DHT length
        0x00, 0x01,  # Table info
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x08,
        0xFF, 0xDA,  # SOS marker
        0x00, 0x08,  # SOS length
        0x01, 0x01, 0x00, 0x00, 0x3F, 0x00,
        0xFF, 0xD9   # EOI marker
    ])
    
    try:
        with open(output_path, 'wb') as f:
            f.write(jpeg_data)
        return True
    except Exception as e:
        print(f"{Fore.RED}Error creating simple JPEG: {e}{Style.RESET_ALL}")
        return False


def create_minimal_dng(output_path):
    """
    Create a minimal DNG (RAW) file.
    Note: This creates a very basic TIFF file with DNG-like structure.
    """
    # Minimal TIFF header for DNG-like file
    tiff_header = bytes([
        # TIFF Header
        0x49, 0x49,  # Little endian
        0x2A, 0x00,  # TIFF magic number
        0x08, 0x00, 0x00, 0x00,  # Offset to first IFD
        
        # IFD (Image File Directory)
        0x03, 0x00,  # Number of directory entries
        
        # Entry 1: ImageWidth
        0x00, 0x01, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00,
        
        # Entry 2: ImageLength  
        0x01, 0x01, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00,
        
        # Entry 3: DNGVersion (makes it a DNG file)
        0x02, 0xC6, 0x01, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x04, 0x00, 0x00,
        
        # Next IFD offset (0 = last IFD)
        0x00, 0x00, 0x00, 0x00
    ])
    
    try:
        with open(output_path, 'wb') as f:
            f.write(tiff_header)
        return True
    except Exception as e:
        print(f"{Fore.RED}Error creating DNG file: {e}{Style.RESET_ALL}")
        return False


def create_minimal_mp4(output_path, duration_seconds=1):
    """
    Create a minimal MP4 file using ffmpeg if available.
    Falls back to a basic MP4 structure if ffmpeg is not available.
    """
    # Try using ffmpeg first
    try:
        result = subprocess.run([
            'ffmpeg', '-f', 'lavfi', '-i', f'color=black:size=320x240:duration={duration_seconds}',
            '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '28',
            '-y', str(output_path)
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            return True
        else:
            print(f"{Fore.YELLOW}ffmpeg failed, creating minimal MP4 structure{Style.RESET_ALL}")
            
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print(f"{Fore.YELLOW}ffmpeg not available, creating minimal MP4 structure{Style.RESET_ALL}")
    
    # Fallback: create minimal MP4 file structure
    # This is a very basic MP4 file that may not play but has correct headers
    mp4_data = bytes([
        # ftyp box (file type)
        0x00, 0x00, 0x00, 0x20,  # box size
        0x66, 0x74, 0x79, 0x70,  # 'ftyp'
        0x69, 0x73, 0x6F, 0x6D,  # major brand 'isom'
        0x00, 0x00, 0x02, 0x00,  # minor version
        0x69, 0x73, 0x6F, 0x6D,  # compatible brand 'isom'
        0x69, 0x73, 0x6F, 0x32,  # compatible brand 'iso2'
        0x61, 0x76, 0x63, 0x31,  # compatible brand 'avc1'
        0x6D, 0x70, 0x34, 0x31,  # compatible brand 'mp41'
        
        # mdat box (media data) - minimal
        0x00, 0x00, 0x00, 0x08,  # box size
        0x6D, 0x64, 0x61, 0x74,  # 'mdat'
    ])
    
    try:
        with open(output_path, 'wb') as f:
            f.write(mp4_data)
        return True
    except Exception as e:
        print(f"{Fore.RED}Error creating MP4 file: {e}{Style.RESET_ALL}")
        return False


def create_legacy_avi(output_path):
    """
    Create a minimal AVI file with MJPEG codec (legacy format).
    """
    try:
        result = subprocess.run([
            'ffmpeg', '-f', 'lavfi', '-i', 'color=black:size=160x120:duration=1',
            '-c:v', 'mjpeg', '-q:v', '10',
            '-y', str(output_path)
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            return True
            
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    
    # Fallback: create minimal AVI structure
    avi_header = bytes([
        # RIFF header
        0x52, 0x49, 0x46, 0x46,  # 'RIFF'
        0x00, 0x01, 0x00, 0x00,  # file size (will be wrong but minimal)
        0x41, 0x56, 0x49, 0x20,  # 'AVI '
        
        # LIST hdrl
        0x4C, 0x49, 0x53, 0x54,  # 'LIST'
        0x20, 0x00, 0x00, 0x00,  # size
        0x68, 0x64, 0x72, 0x6C,  # 'hdrl'
        
        # avih chunk (main AVI header)
        0x61, 0x76, 0x69, 0x68,  # 'avih'
        0x38, 0x00, 0x00, 0x00,  # size (56 bytes)
        # Minimal avih data (56 bytes of mostly zeros)
    ] + [0x00] * 56)
    
    try:
        with open(output_path, 'wb') as f:
            f.write(avi_header)
        return True
    except Exception as e:
        print(f"{Fore.RED}Error creating AVI file: {e}{Style.RESET_ALL}")
        return False


def create_corrupted_file(output_path, base_type='video'):
    """
    Create a corrupted file by writing invalid data with correct extension.
    Creates files that will cause ffprobe/PIL to fail during analysis.
    """
    file_ext = Path(output_path).suffix.lower()
    
    if base_type == 'video' or file_ext in ['.mp4', '.avi', '.mov', '.mkv']:
        # Create corrupted video file - start with some valid-looking headers then corrupt
        if file_ext == '.mp4':
            # Start with partial MP4 header, then corrupt it
            corrupted_data = bytes([
                # Partial ftyp box that will confuse ffprobe
                0x00, 0x00, 0x00, 0x20,  # box size
                0x66, 0x74, 0x79, 0x70,  # 'ftyp'
                0x69, 0x73, 0x6F, 0x6D,  # major brand 'isom'
                # Corrupt the rest - random bytes that break the format
                0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00,
                0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
                0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
                # Sudden end (truncated/corrupted)
            ])
        else:  # AVI
            # Start with partial AVI header, then corrupt it
            corrupted_data = bytes([
                # Partial RIFF header
                0x52, 0x49, 0x46, 0x46,  # 'RIFF'
                0xFF, 0xFF, 0xFF, 0xFF,  # Invalid size
                0x41, 0x56, 0x49, 0x20,  # 'AVI '
                # Corrupt data that will break parsing
                0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
                0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
                # More random corruption
                0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
            ])
    else:
        # Create corrupted image file
        if file_ext in ['.jpg', '.jpeg']:
            # Create a JPEG that starts correctly but is severely corrupted
            corrupted_data = bytes([
                0xFF, 0xD8,  # SOI marker (valid start)
                0xFF, 0xE0,  # APP0 marker (valid)
                0x00, 0x10,  # Valid APP0 length
                0x4A, 0x46, 0x49, 0x46, 0x00,  # JFIF identifier (valid)
                0x01, 0x01,  # JFIF version
                0x01,  # Aspect ratio units
                0x00, 0x01, 0x00, 0x01,  # X and Y density
                0x00, 0x00,  # Thumbnail width and height
                # Now start corruption - invalid marker and data
                0xFF, 0xFF,  # Invalid marker (0xFFFF doesn't exist)
                0xDE, 0xAD, 0xBE, 0xEF,  # Random data
                0xCA, 0xFE, 0xBA, 0xBE,  # More random data
                0xFF, 0xFF, 0xFF, 0xFF,  # Invalid continuation
                # File abruptly ends without proper EOI marker - truncation
            ])
        else:
            # Generic corruption for other file types
            corrupted_data = bytes([
                # Random bytes that don't form any valid file format
                0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
                0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00,
                0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
                0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
            ])
    
    try:
        with open(output_path, 'wb') as f:
            f.write(corrupted_data)
        return True
    except Exception as e:
        print(f"{Fore.RED}Error creating corrupted file: {e}{Style.RESET_ALL}")
        return False


def setup_test_data(output_dir="test_data", cleanup=False):
    """
    Create comprehensive test data structure.
    """
    output_path = Path(output_dir)
    
    if cleanup and output_path.exists():
        print(f"{Fore.YELLOW}Cleaning up existing test data...{Style.RESET_ALL}")
        shutil.rmtree(output_path)
    
    print(f"{Fore.CYAN}Creating test data structure in: {output_path.absolute()}{Style.RESET_ALL}")
    
    # Create directory structure
    directories = [
        "photos/2023/summer",
        "photos/2023/summer/camera1",  # Subdirectory for move_to_dirs testing
        "photos/2023/summer/camera2",  # Subdirectory for move_to_dirs testing
        "photos/2023/winter", 
        "photos/2024/vacation",
        "photos/2024/vacation/day1",   # Subdirectory for move_to_dirs testing
        "photos/raw_files",
        "photos/raw_files/canon",      # RAW subdirectory for move_to_dirs testing
        "photos/raw_files/nikon",      # RAW subdirectory for move_to_dirs testing
        "photos/no_metadata",
        "videos/2023/family",
        "videos/2023/family/kids",     # Subdirectory for move_to_dirs testing
        "videos/2024/events",
        "videos/legacy",
        "videos/corrupted",
        "documents",
        "system_files",
        "mixed_content",
        "mixed_content/subfolder1",    # Subdirectory for move_to_dirs testing
        "mixed_content/subfolder2"     # Subdirectory for move_to_dirs testing
    ]
    
    for dir_path in directories:
        (output_path / dir_path).mkdir(parents=True, exist_ok=True)
    
    # Define test dates with corresponding mtime
    dates = {
        'recent': datetime(2024, 6, 15, 14, 30, 0),
        'old': datetime(2023, 3, 20, 10, 15, 0),
        'older': datetime(2022, 12, 25, 16, 45, 0)
    }
    
    # Directory-specific dates for mtime setting
    dir_dates = {
        'photos/2023/summer': dates['old'],
        'photos/2023/winter': dates['older'], 
        'photos/2024/vacation': dates['recent'],
        'photos/raw_files': dates['old'],  # Mixed dates, use old as default
        'photos/no_metadata': dates['recent'],  # Recent files without metadata
        'videos/2023/family': dates['old'],
        'videos/2024/events': dates['recent'],
        'videos/legacy': dates['older'],  # Old legacy formats
        'videos/corrupted': dates['recent'],  # Recent corruption issues
        'mixed_content': dates['recent'],
        'documents': dates['recent'],
        'system_files': dates['recent']
    }
    
    files_created = 0
    
    print(f"\n{Fore.GREEN}Creating test files:{Style.RESET_ALL}")
    
    # 1. RAW photos (DNG format)
    raw_files = [
        ("photos/raw_files/IMG_001.dng", dates['old']),
        ("photos/raw_files/IMG_002.dng", dates['old']),
        ("photos/raw_files/canon/CR3_001.dng", dates['old']),          # In subdirectory
        ("photos/raw_files/canon/CR3_002.dng", dates['old']),          # In subdirectory
        ("photos/raw_files/canon/IMG_001.dng", dates['old']),          # CONFLICT: same name as parent
        ("photos/raw_files/nikon/NEF_001.dng", dates['old']),          # In subdirectory
        ("photos/raw_files/nikon/NEF_002.dng", dates['old']),          # In subdirectory
        ("photos/raw_files/nikon/IMG_002.dng", dates['old']),          # CONFLICT: same name as parent
        ("photos/2024/vacation/DSC_001.dng", dates['recent'])
    ]
    
    for raw_file, file_date in raw_files:
        file_path = output_path / raw_file
        if create_minimal_dng(file_path):
            set_file_mtime(file_path, file_date)
            print(f"  ✓ RAW: {raw_file}")
            files_created += 1
    
    # 2. JPEG photos with metadata - including suffix variations
    jpeg_files = [
        ("photos/2023/summer/IMG_001.jpg", dates['old']),
        ("photos/2023/summer/IMG_001_edited.jpg", dates['old']),      # Suffix testing
        ("photos/2023/summer/IMG_001_720p.jpg", dates['old']),        # Suffix testing
        ("photos/2023/summer/IMG_002.jpg", dates['old']),
        ("photos/2023/summer/IMG_002_final.jpg", dates['old']),       # Suffix testing
        ("photos/2023/summer/camera1/IMG_001_cam1.jpg", dates['old']),  # In subdirectory
        ("photos/2023/summer/camera1/IMG_002_cam1.jpg", dates['old']),  # In subdirectory
        ("photos/2023/summer/camera1/IMG_001.jpg", dates['old']),       # CONFLICT: same name as parent
        ("photos/2023/summer/camera2/IMG_001_cam2.jpg", dates['old']),  # In subdirectory
        ("photos/2023/summer/camera2/IMG_001.jpg", dates['old']),       # CONFLICT: same name as parent
        ("photos/2023/winter/IMG_003.jpg", dates['older']),
        ("photos/2023/winter/IMG_003_bw.jpg", dates['older']),        # Suffix testing (black & white)
        ("photos/2024/vacation/IMG_004.jpg", dates['recent']),
        ("photos/2024/vacation/IMG_004_HDR.jpg", dates['recent']),    # Suffix testing
        ("photos/2024/vacation/day1/IMG_005_day1.jpg", dates['recent']),  # In subdirectory
        ("photos/2024/vacation/day1/IMG_004.jpg", dates['recent']),       # CONFLICT: same name as parent
        ("mixed_content/photo1.jpg", dates['recent']),
        ("mixed_content/photo1_thumb.jpg", dates['recent']),          # Suffix testing
        ("mixed_content/subfolder1/photo_sub1.jpg", dates['recent']),   # In subdirectory
        ("mixed_content/subfolder1/photo1.jpg", dates['recent']),       # CONFLICT: same name as parent
        ("mixed_content/subfolder2/photo_sub2.jpg", dates['recent']),   # In subdirectory
        ("mixed_content/subfolder2/photo1.jpg", dates['recent'])        # CONFLICT: same name as parent
    ]
    
    for jpeg_file, creation_date in jpeg_files:
        file_path = output_path / jpeg_file
        if create_minimal_jpeg_with_exif(file_path, creation_date):
            set_file_mtime(file_path, creation_date)
            print(f"  ✓ JPEG with EXIF: {jpeg_file}")
            files_created += 1
    
    # 3. Photos without creation metadata
    no_metadata_files = [
        ("photos/no_metadata/no_date_1.jpg", dates['recent']),
        ("photos/no_metadata/no_date_2.jpg", dates['recent']),
        ("mixed_content/no_meta.jpg", dates['recent'])
    ]
    
    for no_meta_file, file_date in no_metadata_files:
        file_path = output_path / no_meta_file
        if create_minimal_jpeg_with_exif(file_path, None):
            set_file_mtime(file_path, file_date)
            print(f"  ✓ JPEG without metadata: {no_meta_file}")
            files_created += 1
    
    # 4. Modern video files (H264/MP4) - with different bitrates for testing
    modern_videos = [
        ("videos/2023/family/family_dinner.mp4", 2, dates['old']),
        ("videos/2023/family/family_dinner_720p.mp4", 1, dates['old']),             # Suffix testing
        ("videos/2023/family/family_dinner_1080p.mp4", 3, dates['old']),            # Suffix testing
        ("videos/2023/family/kids/kids_playing.mp4", 1, dates['old']),              # In subdirectory
        ("videos/2023/family/kids/family_dinner.mp4", 1, dates['old']),             # CONFLICT: same name as parent
        ("videos/2024/events/birthday.mp4", 2, dates['recent']),
        ("videos/2024/events/birthday_4K.mp4", 5, dates['recent']),                    # Suffix testing (high bitrate)
        ("mixed_content/video1.mp4", 1, dates['recent']),
        ("mixed_content/video1_compressed.mp4", 1, dates['recent']),                   # Suffix testing
        ("mixed_content/subfolder1/video_sub1.mp4", 1, dates['recent']),               # In subdirectory
        ("mixed_content/subfolder1/video1.mp4", 1, dates['recent']),                   # CONFLICT: same name as parent
        ("mixed_content/subfolder2/video1.mp4", 1, dates['recent'])                    # CONFLICT: same name as parent
    ]
    
    for video_file, duration, file_date in modern_videos:
        file_path = output_path / video_file
        if create_minimal_mp4(file_path, duration):
            set_file_mtime(file_path, file_date)
            print(f"  ✓ Modern video (MP4/H264): {video_file}")
            files_created += 1
    
    # 5. Legacy video format
    legacy_videos = [
        ("videos/legacy/old_video.avi", dates['older']),
        ("videos/legacy/ancient_clip.avi", dates['older'])
    ]
    
    for legacy_video, file_date in legacy_videos:
        file_path = output_path / legacy_video
        if create_legacy_avi(file_path):
            set_file_mtime(file_path, file_date)
            print(f"  ✓ Legacy video (AVI/MJPEG): {legacy_video}")
            files_created += 1
    
    # 6. Corrupted video files
    corrupted_videos = [
        ("videos/corrupted/broken1.mp4", dates['recent']),
        ("videos/corrupted/broken2.avi", dates['recent'])
    ]
    
    for corrupted_video, file_date in corrupted_videos:
        file_path = output_path / corrupted_video
        if create_corrupted_file(file_path, 'video'):
            set_file_mtime(file_path, file_date)
            print(f"  ✓ Corrupted video: {corrupted_video}")
            files_created += 1
    
    # 7. Corrupted photo files
    corrupted_photos = [
        ("photos/no_metadata/corrupted.jpg", dates['recent']),
        ("mixed_content/broken.jpg", dates['recent'])
    ]
    
    for corrupted_photo, file_date in corrupted_photos:
        file_path = output_path / corrupted_photo
        if create_corrupted_file(file_path, 'photo'):
            set_file_mtime(file_path, file_date)
            print(f"  ✓ Corrupted photo: {corrupted_photo}")
            files_created += 1
    
    # 8. System/document files
    system_files = [
        ("documents/readme.txt", "This is a test document file.\nCreated for immich_tools testing.\n", dates['recent']),
        ("documents/report.pdf", "Fake PDF content - not a real PDF file.\n", dates['recent']),
        ("system_files/.DS_Store", "Fake macOS metadata file\n", dates['recent']),
        ("system_files/Thumbs.db", "Fake Windows thumbnail cache\n", dates['recent']),
        ("mixed_content/notes.txt", "Mixed content directory notes\n", dates['recent']),
        ("mixed_content/subfolder1/notes_sub1.txt", "Notes in subfolder 1\n", dates['recent']),
        ("mixed_content/subfolder1/notes.txt", "CONFLICT: Same name as parent notes\n", dates['recent']),  # CONFLICT
        ("mixed_content/subfolder2/notes_sub2.txt", "Notes in subfolder 2\n", dates['recent']),
        ("mixed_content/subfolder2/notes.txt", "CONFLICT: Same name as parent notes\n", dates['recent']),  # CONFLICT
        ("photos/2023/summer/info.txt", "Photo session info\n", dates['old']),
        ("photos/2023/summer/camera1/camera1_info.txt", "Camera 1 session info\n", dates['old']),
        ("photos/2023/summer/camera1/info.txt", "CONFLICT: Camera 1 general info\n", dates['old']),    # CONFLICT
        ("photos/2023/summer/camera2/camera2_info.txt", "Camera 2 session info\n", dates['old']),
        ("photos/2023/summer/camera2/info.txt", "CONFLICT: Camera 2 general info\n", dates['old']),    # CONFLICT
        ("photos/2024/vacation/day1/day1_log.txt", "Day 1 vacation log\n", dates['recent']),
        ("photos/raw_files/canon/canon_settings.txt", "Canon camera settings\n", dates['old']),
        ("photos/raw_files/nikon/nikon_settings.txt", "Nikon camera settings\n", dates['old']),
        ("videos/2023/family/kids/kids_notes.txt", "Notes about kids videos\n", dates['old'])
    ]
    
    for sys_file, content, file_date in system_files:
        file_path = output_path / sys_file
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            set_file_mtime(file_path, file_date)
            print(f"  ✓ System file: {sys_file}")
            files_created += 1
        except Exception as e:
            print(f"  ✗ Failed to create {sys_file}: {e}")
    
    # Create a summary file
    summary_content = f"""# Test Data Summary
Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Total files: {files_created}

## Structure:
- photos/2023/summer/ - JPEG photos with 2023 dates + info.txt
  - photos/2023/summer/camera1/ - Additional JPEG photos (for move_to_dirs testing)
  - photos/2023/summer/camera2/ - Additional JPEG photos (for move_to_dirs testing)
- photos/2023/winter/ - JPEG photos with 2022 dates  
- photos/2024/vacation/ - Recent JPEG photos + RAW files
  - photos/2024/vacation/day1/ - Photos from specific day (for move_to_dirs testing)
- photos/raw_files/ - DNG (RAW) photo files
- photos/no_metadata/ - JPEG photos without creation dates + corrupted photo
- videos/2023/family/ - Modern MP4 videos
  - videos/2023/family/kids/ - Kids videos (for move_to_dirs testing)
- videos/2024/events/ - Modern MP4 videos
- videos/legacy/ - Old AVI/MJPEG videos
- videos/corrupted/ - Corrupted video files
- documents/ - Text and PDF files
- system_files/ - System metadata files (.DS_Store, Thumbs.db)
- mixed_content/ - Mixed file types for complex scenarios
  - mixed_content/subfolder1/ - Mixed content subdirectory (for move_to_dirs testing)
  - mixed_content/subfolder2/ - Mixed content subdirectory (for move_to_dirs testing)

## File Types:
- JPEG with EXIF metadata (creation dates)
- JPEG without metadata
- RAW photos (DNG format)
- Modern videos (MP4/H264)
- Legacy videos (AVI/MJPEG)
- Corrupted media files
- System/document files

## Testing Features:
- Subdirectories with files for testing move_to_dirs.py functionality
- Files with same names in different subdirectories (conflict testing)
- Files with various suffixes (_edited, _720p, _HDR, etc.) for suffix testing
- Videos with different durations (simulating different bitrates)
- Mixed content directories for complex scenarios
- Various date ranges for metadata testing
- RAW files organized by camera brand (canon/nikon subdirectories)
- Comprehensive conflict scenarios for move_to_dirs.py testing

## Test Scenarios Covered:
1. **media_analyzer.py**: All file types, corrupted files, various formats
2. **media_query.py**: Directory structure analysis, size filtering, file categorization
3. **move_to_dirs.py**: Subdirectories, name conflicts, mixed file types
4. **video_encoder.py**: Modern vs legacy formats, various durations
5. **photo_converter.py**: RAW files, JPEG with/without metadata
6. **Suffix testing**: Files with _720p, _edited, _HDR, _thumb, etc. suffixes
7. **High bitrate testing**: Videos with longer durations (higher bitrate simulation)
8. **Duplicate detection**: Files with same names in different locations

Use this test data to verify all immich_tools functionality.
"""
    
    summary_path = output_path / "TEST_DATA_SUMMARY.md"
    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(summary_content)
        set_file_mtime(summary_path, dates['recent'])
        files_created += 1
    except Exception as e:
        print(f"  ✗ Failed to create summary: {e}")
    
    print(f"\n{Fore.GREEN}✅ Test data setup complete!{Style.RESET_ALL}")
    print(f"Created {files_created} files in {len(directories)} directories")
    print(f"Location: {output_path.absolute()}")
    print(f"\nRead {summary_path} for detailed structure information.")
    
    return files_created


def main():
    parser = argparse.ArgumentParser(
        description='Setup test data structure for immich_tools',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--output-dir',
        default='test_data',
        help='Output directory for test data (default: test_data)'
    )
    
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Remove existing test data before creating new'
    )
    
    args = parser.parse_args()
    
    try:
        files_created = setup_test_data(args.output_dir, args.cleanup)
        return 0 if files_created > 0 else 1
        
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Setup interrupted by user{Style.RESET_ALL}")
        return 1
    except Exception as e:
        print(f"\n{Fore.RED}Error during setup: {e}{Style.RESET_ALL}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
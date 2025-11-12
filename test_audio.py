#!/usr/bin/env python3
"""
Quick test script to verify audio metadata extraction

This script demonstrates how to:
1. Extract audio metadata using get_audio_metadata()
2. Save audio metadata to database
3. Query audio files from database

Usage:
    # First, create test audio files
    cd test && python setup_test_data.py --output-dir test_data
    
    # Then analyze them
    python media_analyzer.py test/test_data/audio --database test_audio.db
    
    # Check stats
    python media_analyzer.py test/test_data --database test_audio.db --stats
"""

import sys
import json
import sqlite3
from pathlib import Path

# Add parent directory to path to import local libraries
sys.path.insert(0, str(Path(__file__).parent))

from lib.metadata import get_audio_metadata

def test_audio_metadata_extraction(audio_file_path):
    """Test audio metadata extraction from a file"""
    print(f"Testing audio file: {audio_file_path}")
    
    try:
        metadata = get_audio_metadata(audio_file_path)
        print("\n✓ Audio metadata extracted successfully:")
        print(json.dumps(metadata, indent=2))
        
        if 'audio_metadata' in metadata:
            print("\n✓ Audio-specific metadata (artist, album, etc.):")
            print(json.dumps(metadata['audio_metadata'], indent=2))
        else:
            print("\n⚠ No audio-specific metadata found")
            
        return True
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False

def query_audio_from_database(db_path):
    """Query audio files from the database"""
    if not Path(db_path).exists():
        print(f"Database not found: {db_path}")
        print("\nTo create database, run:")
        print(f"  python media_analyzer.py test/test_data/audio --database {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all audio files with metadata
    cursor.execute("""
        SELECT file_name, duration, codec_name, bit_rate, metadata
        FROM media_files 
        WHERE media_type = 'audio' AND is_corrupted = 0
        ORDER BY file_name
    """)
    
    audio_files = cursor.fetchall()
    
    if not audio_files:
        print("No audio files found in database")
        conn.close()
        return
    
    print(f"\n✓ Found {len(audio_files)} audio files in database:\n")
    
    for file_name, duration, codec, bitrate, metadata_json in audio_files:
        print(f"File: {file_name}")
        print(f"  Duration: {duration:.2f}s")
        print(f"  Codec: {codec}")
        print(f"  Bitrate: {bitrate}")
        
        if metadata_json:
            metadata = json.loads(metadata_json)
            print(f"  Metadata:")
            for key, value in metadata.items():
                print(f"    {key}: {value}")
        print()
    
    conn.close()

if __name__ == "__main__":
    print("Audio Support Test for immich_tools\n")
    print("=" * 50)
    
    # Check if test data exists
    test_audio_dir = Path("test/test_data/audio")
    
    if not test_audio_dir.exists():
        print("\n⚠ Test data not found. Creating test data...")
        print("\nRun:")
        print("  cd test && python setup_test_data.py --output-dir test_data\n")
        sys.exit(1)
    
    # Find an audio file to test
    audio_files = list(test_audio_dir.rglob("*.mp3")) + list(test_audio_dir.rglob("*.m4a"))
    audio_files = [f for f in audio_files if 'corrupted' not in str(f)]
    
    if audio_files:
        print(f"\nTesting metadata extraction from: {audio_files[0]}")
        test_audio_metadata_extraction(str(audio_files[0]))
    
    # Query database if it exists
    db_path = "test_audio.db"
    print(f"\n{'=' * 50}")
    print(f"\nQuerying database: {db_path}")
    query_audio_from_database(db_path)
    
    print("\n" + "=" * 50)
    print("\nTo analyze all audio files, run:")
    print(f"  python media_analyzer.py test/test_data/audio --database {db_path}")
    print("\nTo see statistics:")
    print(f"  python media_analyzer.py test/test_data --database {db_path} --stats")


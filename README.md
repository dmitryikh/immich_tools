# Immich Tools

A set of command-line tools for managing bulk media files before or after they are uploaded to Immich.

## Features

- **Organize media library**: Find photos by resolution and create albums
- **Remove duplicates**: Detect and delete duplicate video files  
- **Video analysis**: Analyze video metadata (codec, resolution, bitrate)
- **Video compression**: Encode high bitrate videos with embedded FFmpeg
- **Bulk operations**: Process thousands of files efficiently

## Architecture

All tools are packaged in a single Docker container with embedded dependencies:
- **Python 3.12** with all required packages
- **FFmpeg** for video processing
- **ExifTool** for metadata manipulation
- **RawTherapee** for RAW image processing
- **ImageMagick** for image operations


## Installation

```bash
# Clone the repository
git clone <repository-url>
cd immich_tools

# Build the Docker container with all tools embedded
docker build -t immich_tools .
```

The container includes all necessary tools (Python, FFmpeg, ExifTool, RawTherapee, ImageMagick) and dependencies pre-installed. No additional setup required!

## Usage Pattern

All commands follow this pattern:
```bash
docker run --rm -v "/path/to/your/data:/data" immich_tools script_name.py [arguments]
```

Where:
- `/path/to/your/data` - absolute path to your media files or workspace
- `/data` - mount point inside the container (always use `/data` in script arguments)
- `script_name.py` - any of the Python scripts in this repository

For Immich API operations, also mount your configuration:
```bash
docker run --rm -v "/path/to/config:/config" -v "/path/to/media:/data" immich_tools script_name.py [arguments]
```

## Use cases

### Find screenshots in the Immich library and assign them to a new album
```bash
docker run --rm -v "/path/to/your/config:/config" immich_tools album_by_resolution.py 1179x2556 "Screenshots"
```

### Assign datetime meta for Immich images based on the file name
The command will parse all image assets from `Album Name` if creation time is targeted `2025` the script will attempt to parse creation time based on the filename (e.g. `2018-03-10 21-30-06.JPG`) and assign it to the asset in Immich.
```bash
docker run --rm -v "/path/to/your/config:/config" immich_tools date_from_name.py --album "Album Name" --target-year 2025 --dry-run
```

### Restore metadata from file path
Before loading image and video files to Immich, one can run this tool to update file metadata (if it's not set) based on the path. The datetime information should be encoded in the path. See the comment to `assign_creation_time.py` for supported formats.
```bash
# Warning! This tool overwrite files inplace. Ensure you have backup copy before proceed.
docker run --rm -v "/path/to/your/library:/data" immich_tools assign_creation_time.py /data --verbose
```

### Collect data on video files in the directory (recursively)
```bash
docker run --rm -v "/path/to/your/media:/data" immich_tools video_analyzer.py /data --database /data/video_analysis.db --workers 16

# Quick stats on already collected data
docker run --rm -v "/path/to/your/media:/data" immich_tools video_analyzer.py /data --stats --database /data/video_analysis.db
```

### Export lists of files with large bitrates and duplicates

```bash
# Only files with 'Camera Uploads' in the path will be added to the list for deletion.
docker run --rm -v "/path/to/your/media:/data" immich_tools video_query.py --database /data/video_analysis.db --export-duplicates /data/duplicates_by_hash.txt --export-pattern 'Camera Uploads'

docker run --rm -v "/path/to/your/media:/data" immich_tools video_query.py --database /data/video_analysis.db --export-list /data/high_quality_files.txt --export-min-bitrate 15 --export-min-size 50

# Export files with specific suffix that have corresponding originals (e.g., transcoded versions)
docker run --rm -v "/path/to/your/media:/data" immich_tools video_query.py --database /data/video_analysis.db --export-with-suffix /data/transcoded_files.txt --suffix "_720p"
```


### Delete duplicate video files
```bash
# Be careful! This will do the real deletion! Use --dry-run first.
docker run -it --rm -v "/path/to/your/media:/data" immich_tools delete_files.py /data/duplicates_by_hash.txt --pattern "Camera Uploads" --dry-run
```


### Encode video files with large bitrate
This command will create new files next to the original one but with the `_720p.mp4` as a filename suffix. All tools are now embedded in the container for optimal performance.

```bash
docker run --rm -v "/path/to/your/media:/data" immich_tools video_encoder.py --suffix=_720p /data/high_quality_files.txt

# After you ensure the transcoded quality is good enough:
# Be careful! This will do the real deletion! Use --dry-run first.
docker run --rm -v "/path/to/your/media:/data" immich_tools delete_files.py /data/high_quality_files.txt --dry-run
```

### Convert RAW photos to JPEG
Convert RAW image files to high-quality JPEG format using RawTherapee with parallel processing:

```bash
# Convert all RAW files with .RW2 extension
docker run --rm -v "/path/to/your/media:/data" immich_tools photo_converter.py --pattern ".RW2" --max-workers 8 --quality 85 /data/raw_files_list.txt

# Convert specific file types with custom settings
docker run --rm -v "/path/to/your/media:/data" immich_tools photo_converter.py --pattern ".CR2" --quality 95 --suffix "_converted" /data/raw_files.txt
```

# Immich Tools

A set of command-line tools for managing bulk media files before or after they are uploaded to Immich.

## Features

- **Organize media library**: Find photos by resolution and create albums
- **Remove duplicates**: Detect and delete duplicate video files  
- **Video analysis**: Analyze video metadata (codec, resolution, bitrate)
- **Video compression**: Encode high bitrate videos using Docker FFmpeg
- **Bulk operations**: Process thousands of files efficiently

```bash
# Clone and setup environment
git clone <repository-url>
cd immich_tools

# Create virtual environment
python3 -m venv venv
source venv/bin/activate    # macOS/Linux
# or: venv\Scripts\activate # Windows

# Install dependencies
pip install -r requirements.txt

# Setup configuration
cp .env.example .env
# Edit .env with your Immich server URL and API key
```

## Use cases

### Find screenshots in the Immich library and assign them to a new album
```bash
python album_by_resolution.py 1179x2556 "Screenshots"
```

### Assign datetime meta for Immich images based on the file name
The command will parse all image assets from `Album Name` if creation time is targeted `2025` the script will attempt to parse creation time based on the filename (e.g. `2018-03-10 21-30-06.JPG`) and assign it to the asset in Immich.
```
python date_from_name.py --album "Album Name" --target-year 2025 --dry-run
```

### Collect data on video files in the directory (recoursively)
```bash
python video_analyzer.py folder_to_analyze --database video_analysis.db --workers 16

# quick stats on already collected data
python video_analyzer.py folder_to_analyze --stats --database video_analysis.db
```

### Export lists of files with large bitrates and duplicates

```bash
# Only files with 'Camera Uploads' in the path will be added to the list for deletion.
python video_query.py --database video_analysis.db --export-duplicates duplicates_by_hash.txt --export-pattern 'Camera Uploads'

python video_query.py --database video_analysis.db --export-list high_quality_files.txt --export-min-bitrate 15 --export-min-size 50
```


### Delete duplicate video files
```bash
# Be carefull! This will do the real deletion! Use --dry-run first.
python delete_files.py duplicates_by_hash.txt --pattern "Camera Uploads" --dry-run
```


### Encode video files with large bitrate
This command will create new files next to the original one but with the `_720p.mp4` as a filename suffix. The command uses docker container for running ffmpeg (so you don't need to install it on your machine).

```bash
python3 video_encoder.py --suffix=_720p high_quality_files.txt

# After you ensure the transcoded quaility is good enough:
# Be carefull! This will do the real deletion! Use --dry-run first.
python delete_files.py high_quality_files.txt --dry-run
```

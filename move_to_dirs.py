#!/usr/bin/env python3
"""
Move files from subdirectories to parent directory level.

This script recursively finds all files in subdirectories of specified directories
and moves them to the parent directory level. If file names conflict, adds suffixes.

Usage examples:
    # Dry run (show what would be moved without actually moving)
    python move_to_dirs.py --dry-run "/path/to/dir1" "/path/to/dir2"
    
    # Actually move files
    python move_to_dirs.py "/path/to/dir1" "/path/to/dir2"
    
    # Process directories from a file list
    python move_to_dirs.py --from-file directories.txt --dry-run

Example:
    Before:
        /data/photos/event/
        ├── subdir1/
        │   ├── IMG_001.jpg
        │   └── IMG_002.jpg
        └── subdir2/
            └── IMG_003.jpg
    
    After:
        /data/photos/event/
        ├── IMG_001.jpg
        ├── IMG_002.jpg
        ├── IMG_003.jpg
        ├── subdir1/  (empty)
        └── subdir2/  (empty)
"""

import os
import sys
import shutil
import argparse
from pathlib import Path
from collections import defaultdict
from colorama import Fore, Style, init

# Initialize colorama for cross-platform colored output
init(autoreset=True)


def format_file_size(size_bytes):
    """Convert bytes to human readable format."""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    import math
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 1)
    return f"{s} {size_names[i]}"


def get_unique_filename(target_dir, filename, used_filenames=None):
    """
    Generate a unique filename in target directory.
    If filename exists, add suffix like '_1', '_2', etc.
    Also checks against used_filenames set to avoid conflicts during batch processing.
    """
    if used_filenames is None:
        used_filenames = set()
    
    target_path = Path(target_dir) / filename
    if not target_path.exists() and filename not in used_filenames:
        return filename
    
    # Split filename and extension
    name_part = Path(filename).stem
    ext_part = Path(filename).suffix
    
    counter = 1
    while True:
        new_filename = f"{name_part}_{counter}{ext_part}"
        new_target_path = Path(target_dir) / new_filename
        if not new_target_path.exists() and new_filename not in used_filenames:
            return new_filename
        counter += 1


def collect_files_to_move(directory):
    """
    Collect all files from subdirectories that need to be moved.
    Returns list of (source_path, relative_subdir) tuples.
    """
    directory = Path(directory)
    files_to_move = []
    
    if not directory.exists():
        print(f"{Fore.RED}Error: Directory does not exist: {directory}{Style.RESET_ALL}")
        return files_to_move
    
    if not directory.is_dir():
        print(f"{Fore.RED}Error: Not a directory: {directory}{Style.RESET_ALL}")
        return files_to_move
    
    # Find all files in subdirectories (not in the root directory)
    for root, dirs, files in os.walk(directory):
        root_path = Path(root)
        
        # Skip the root directory itself
        if root_path == directory:
            continue
        
        # Get relative path from target directory
        relative_subdir = root_path.relative_to(directory)
        
        for filename in files:
            source_path = root_path / filename
            files_to_move.append((source_path, relative_subdir))
    
    return files_to_move


def move_files_from_subdirs(directory, dry_run=True):
    """
    Move all files from subdirectories to the parent directory level.
    
    Args:
        directory: Target directory path
        dry_run: If True, only show what would be moved without actually moving
    
    Returns:
        Tuple of (moved_count, total_size, conflicts_count)
    """
    directory = Path(directory).resolve()
    print(f"\n{Fore.CYAN}Processing directory: {directory}{Style.RESET_ALL}")
    
    # Collect all files to move
    files_to_move = collect_files_to_move(directory)
    
    if not files_to_move:
        print(f"{Fore.YELLOW}  No files found in subdirectories{Style.RESET_ALL}")
        return 0, 0, 0
    
    print(f"  Found {len(files_to_move)} files in subdirectories")
    
    # Group files by their names to detect conflicts
    filename_groups = defaultdict(list)
    for source_path, relative_subdir in files_to_move:
        filename = source_path.name
        filename_groups[filename].append((source_path, relative_subdir))
    
    moved_count = 0
    total_size = 0
    conflicts_count = 0
    used_filenames = set()  # Track filenames we're going to use
    
    # Process each file
    for source_path, relative_subdir in files_to_move:
        try:
            # Get file size
            file_size = source_path.stat().st_size
            total_size += file_size
            
            # Determine target filename (handle conflicts)
            original_filename = source_path.name
            target_filename = get_unique_filename(directory, original_filename, used_filenames)
            
            if target_filename != original_filename:
                conflicts_count += 1
            
            used_filenames.add(target_filename)
            target_path = directory / target_filename
            
            # Show what we're doing
            size_str = format_file_size(file_size)
            if target_filename != original_filename:
                status_color = Fore.YELLOW
                status = f"RENAME: {original_filename} → {target_filename}"
            else:
                status_color = Fore.GREEN
                status = f"MOVE: {original_filename}"
            
            print(f"  {status_color}{status}{Style.RESET_ALL}")
            print(f"    From: {relative_subdir}")
            print(f"    Size: {size_str}")
            
            # Actually move the file (unless dry run)
            if not dry_run:
                shutil.move(str(source_path), str(target_path))
                print(f"    {Fore.GREEN}✓ Moved{Style.RESET_ALL}")
            else:
                print(f"    {Fore.BLUE}[DRY RUN] Would move{Style.RESET_ALL}")
            
            moved_count += 1
            
        except Exception as e:
            print(f"  {Fore.RED}Error processing {source_path}: {e}{Style.RESET_ALL}")
    
    return moved_count, total_size, conflicts_count


def cleanup_empty_dirs(directory, dry_run=True):
    """
    Remove empty subdirectories after moving files.
    
    Args:
        directory: Target directory path
        dry_run: If True, only show what would be removed
    
    Returns:
        Number of directories removed
    """
    directory = Path(directory)
    removed_count = 0
    
    # Find all subdirectories
    subdirs = []
    for root, dirs, files in os.walk(directory):
        for dirname in dirs:
            subdir_path = Path(root) / dirname
            subdirs.append(subdir_path)
    
    # Sort by depth (deepest first) to remove nested empty dirs
    subdirs.sort(key=lambda p: len(p.parts), reverse=True)
    
    for subdir in subdirs:
        try:
            # Check if directory is empty
            if not any(subdir.iterdir()):
                if dry_run:
                    print(f"  {Fore.BLUE}[DRY RUN] Would remove empty dir: {subdir.relative_to(directory)}{Style.RESET_ALL}")
                else:
                    subdir.rmdir()
                    print(f"  {Fore.GREEN}Removed empty dir: {subdir.relative_to(directory)}{Style.RESET_ALL}")
                removed_count += 1
        except Exception as e:
            print(f"  {Fore.RED}Error removing {subdir}: {e}{Style.RESET_ALL}")
    
    return removed_count


def main():
    parser = argparse.ArgumentParser(
        description='Move files from subdirectories to parent directory level',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        'directories',
        nargs='*',
        help='Directories to process'
    )
    
    parser.add_argument(
        '--from-file',
        metavar='FILE',
        help='Read directory list from file (one directory per line)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be moved without actually moving files'
    )
    
    parser.add_argument(
        '--cleanup-empty',
        action='store_true',
        help='Remove empty subdirectories after moving files'
    )
    
    args = parser.parse_args()
    
    # Collect directories to process
    directories_to_process = []
    
    if args.from_file:
        try:
            with open(args.from_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        directories_to_process.append(line)
        except Exception as e:
            print(f"{Fore.RED}Error reading file {args.from_file}: {e}{Style.RESET_ALL}")
            return 1
    
    if args.directories:
        directories_to_process.extend(args.directories)
    
    if not directories_to_process:
        print(f"{Fore.RED}Error: No directories specified{Style.RESET_ALL}")
        print("Use either directory arguments or --from-file option")
        return 1
    
    # Show operation mode
    mode_str = "DRY RUN MODE" if args.dry_run else "LIVE MODE"
    mode_color = Fore.BLUE if args.dry_run else Fore.RED
    print(f"\n{mode_color}=== {mode_str} ==={Style.RESET_ALL}")
    if args.dry_run:
        print("Files will NOT be actually moved. Use without --dry-run to perform actual moves.")
    else:
        print("Files WILL be moved. Use --dry-run first to preview changes.")
    
    # Process each directory
    total_moved = 0
    total_size = 0
    total_conflicts = 0
    total_dirs_removed = 0
    
    for directory in directories_to_process:
        moved_count, size, conflicts = move_files_from_subdirs(directory, args.dry_run)
        total_moved += moved_count
        total_size += size
        total_conflicts += conflicts
        
        # Cleanup empty directories if requested
        if args.cleanup_empty and moved_count > 0:
            print(f"\n{Fore.CYAN}Cleaning up empty directories in: {directory}{Style.RESET_ALL}")
            dirs_removed = cleanup_empty_dirs(directory, args.dry_run)
            total_dirs_removed += dirs_removed
    
    # Show summary
    print(f"\n{Fore.CYAN}=== SUMMARY ==={Style.RESET_ALL}")
    print(f"Directories processed: {len(directories_to_process)}")
    print(f"Files moved: {total_moved}")
    if total_conflicts > 0:
        print(f"Name conflicts resolved: {total_conflicts}")
    print(f"Total size: {format_file_size(total_size)}")
    if args.cleanup_empty:
        print(f"Empty directories removed: {total_dirs_removed}")
    
    if args.dry_run and total_moved > 0:
        print(f"\n{Fore.YELLOW}Run without --dry-run to actually move the files{Style.RESET_ALL}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
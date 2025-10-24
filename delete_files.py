#!/usr/bin/env python3
"""
Utility for deleting files from a list by local path.
WARNING: Use with caution! Files will be permanently deleted!

Usage:
python delete_files.py duplicates_by_hash.txt --pattern "Camera Uploads" --dry-run
"""

import os
import sys
import argparse
from colorama import Fore, Style, init
from lib.utils import sort_files_by_directory_depth

# Initialize colorama with forced colors for container support
init(autoreset=True, strip=False)

def read_file_list(file_path):
    """Reads file list from text file"""
    files = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if line and not line.startswith('#'):
                    files.append(line)
        return files
    except FileNotFoundError:
        print(f"{Fore.RED}❌ File not found: {file_path}{Style.RESET_ALL}")
        return []
    except Exception as e:
        print(f"{Fore.RED}❌ File reading error: {e}{Style.RESET_ALL}")
        return []

def check_files_exist(file_list):
    """Checks which files exist"""
    existing = []
    missing = []
    
    for file_path in file_list:
        if os.path.exists(file_path):
            existing.append(file_path)
        else:
            missing.append(file_path)
    
    return existing, missing

def format_file_size(size_bytes):
    """Formats file size"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"

def calculate_total_size(file_list):
    """Calculates total file size"""
    total_size = 0
    for file_path in file_list:
        try:
            if os.path.exists(file_path):
                total_size += os.path.getsize(file_path)
        except OSError:
            pass  # File may be inaccessible
    return total_size

def delete_files(file_list, dry_run=True):
    """Deletes files from list"""
    success_count = 0
    error_count = 0
    total_freed = 0
    
    if dry_run:
        print(f"{Fore.YELLOW}🔍 PREVIEW MODE (files will NOT be deleted){Style.RESET_ALL}")
    else:
        print(f"{Fore.RED}🗑️  DELETE MODE (files WILL be permanently deleted!){Style.RESET_ALL}")
    
    print(f"\nProcessing {len(file_list)} files:")
    print("-" * 80)

    file_list = sort_files_by_directory_depth(file_list)
    
    for i, file_path in enumerate(file_list, 1):
        try:
            if not os.path.exists(file_path):
                print(f"{i:3}. {Fore.YELLOW}SKIPPED{Style.RESET_ALL} (does not exist): {file_path}")
                continue
            
            file_size = os.path.getsize(file_path)
            size_str = format_file_size(file_size)
            
            if dry_run:
                print(f"{i:3}. {Fore.CYAN}PREVIEW{Style.RESET_ALL} [{size_str}]: {os.path.basename(file_path)} {os.path.dirname(file_path)}")
            else:
                os.remove(file_path)
                print(f"{i:3}. {Fore.RED}DELETED{Style.RESET_ALL} [{size_str}]: {os.path.basename(file_path)} {os.path.dirname(file_path)}")
                total_freed += file_size
            
            success_count += 1
            
        except PermissionError:
            print(f"{i:3}. {Fore.RED}ERROR{Style.RESET_ALL} (no permissions): {file_path}")
            error_count += 1
        except OSError as e:
            print(f"{i:3}. {Fore.RED}ERROR{Style.RESET_ALL} ({e}): {file_path}")
            error_count += 1
    
    # Summary statistics
    print("\n" + "=" * 80)
    if dry_run:
        print(f"{Fore.CYAN}📊 PREVIEW STATISTICS:{Style.RESET_ALL}")
        total_size = calculate_total_size(file_list)
        print(f"  Files to delete: {success_count}")
        print(f"  Space to be freed: {Fore.GREEN}{format_file_size(total_size)}{Style.RESET_ALL}")
        print(f"  Errors: {error_count}")
        print(f"\n{Fore.YELLOW}💡 For actual deletion run without --dry-run{Style.RESET_ALL}")
    else:
        print(f"{Fore.GREEN}✅ DELETION COMPLETED:{Style.RESET_ALL}")
        print(f"  Files deleted: {success_count}")
        print(f"  Space freed: {Fore.GREEN}{format_file_size(total_freed)}{Style.RESET_ALL}")
        print(f"  Errors: {error_count}")
    
    return success_count, error_count

def main():
    parser = argparse.ArgumentParser(
        description='Delete duplicate files from list',
        epilog='WARNING: Be careful! Deleted files cannot be recovered!'
    )
    parser.add_argument(
        'file_list',
        help='Path to file with list of files to delete'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Preview mode - show what would be deleted, but do not delete'
    )
    parser.add_argument(
        '--confirm', '-y',
        action='store_true',
        help='Do not ask for confirmation before deletion'
    )
    parser.add_argument(
        '--pattern',
        help='Delete only files containing specified pattern in path'
    )
    
    args = parser.parse_args()
    
    # Check if list file exists
    if not os.path.exists(args.file_list):
        print(f"{Fore.RED}❌ List file not found: {args.file_list}{Style.RESET_ALL}")
        return 1
    
    # Read file list
    print(f"📋 Reading list from: {args.file_list}")
    file_list = read_file_list(args.file_list)
    
    if not file_list:
        print(f"{Fore.YELLOW}⚠️  File list is empty{Style.RESET_ALL}")
        return 1
    
    print(f"Found {len(file_list)} paths in list")
    
    # Filter by pattern if specified
    if args.pattern:
        original_count = len(file_list)
        file_list = [f for f in file_list if args.pattern in f]
        print(f"After filtering by pattern '{args.pattern}': {len(file_list)} of {original_count}")
    
    if not file_list:
        print(f"{Fore.YELLOW}⚠️  List is empty after filtering{Style.RESET_ALL}")
        return 1
    
    # Check which files exist
    existing_files, missing_files = check_files_exist(file_list)
    
    if missing_files:
        print(f"\n{Fore.YELLOW}⚠️  {len(missing_files)} files from list not found{Style.RESET_ALL}")
    
    if not existing_files:
        print(f"{Fore.YELLOW}❌ No files from list found{Style.RESET_ALL}")
        return 1
    
    print(f"\n{Fore.GREEN}✅ Found {len(existing_files)} existing files{Style.RESET_ALL}")
    
    # Show file examples
    if len(existing_files) > 0:
        print(f"\nExamples of files to delete:")
        for i, file_path in enumerate(existing_files[:5], 1):
            try:
                size = format_file_size(os.path.getsize(file_path))
                print(f"  {i}. [{size}] {os.path.basename(file_path)}")
            except OSError:
                print(f"  {i}. [???] {os.path.basename(file_path)}")
        
        if len(existing_files) > 5:
            print(f"  ... and {len(existing_files) - 5} more files")
    
    # Ask for confirmation if not dry-run and not --confirm
    if not args.dry_run and not args.confirm:
        print(f"\n{Fore.RED}⚠️  WARNING: You are about to delete {len(existing_files)} files!{Style.RESET_ALL}")
        print("This action cannot be undone!")
        
        response = input("\nContinue? (type 'yes' to confirm): ").strip().lower()
        if response != 'yes':
            print("Operation cancelled")
            return 0
    
    # Perform deletion
    success_count, error_count = delete_files(existing_files, args.dry_run)
    
    return 0 if error_count == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
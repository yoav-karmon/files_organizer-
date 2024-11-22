import os
import hashlib
import argparse
from collections import defaultdict
import threading
import time
from shutil import move

class FileProcessor:
    def __init__(self):
        self.files_processed = 0
        self.stop_signal = False

    def start_progress_report(self):
        """Start a background thread to report progress."""
        def report():
            while not self.stop_signal:
                print(f"Files processed so far: {self.files_processed}")
                time.sleep(5)
        threading.Thread(target=report, daemon=True).start()

def calculate_checksum(file_path, algorithm='md5', chunk_size=1024):
    """Calculate the checksum of a file."""
    hash_func = hashlib.new(algorithm)
    with open(file_path, 'rb') as f:
        while chunk := f.read(chunk_size):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def find_duplicates(folder_path, processor, extensions=None, max_depth=None, algorithm='md5', max_size_mb=None):
    """Find duplicate files in a folder based on their checksums, with depth limit, extension filtering, and size limit."""
    checksum_map = defaultdict(list)
    root_depth = folder_path.rstrip(os.sep).count(os.sep)

    for root, dirs, files in os.walk(folder_path):
        # Check depth limit
        current_depth = root.rstrip(os.sep).count(os.sep) - root_depth
        if max_depth is not None and current_depth >= max_depth:
            # Skip deeper directories
            dirs[:] = []  # Clears the list of subdirectories to prevent os.walk from diving deeper

        for file in files:
            if extensions and not file.lower().endswith(tuple(extensions)):
                continue  # Skip files with extensions not in the list

            file_path = os.path.join(root, file)
            try:
                # Check size limit
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                if max_size_mb is not None and file_size_mb > max_size_mb:
                    print(f"Skipping file {file_path} (Size: {file_size_mb:.2f} MB exceeds limit of {max_size_mb} MB)")
                    continue

                checksum = calculate_checksum(file_path, algorithm)
                checksum_map[checksum].append(file_path)
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
            finally:
                processor.files_processed += 1  # Increment the processed count
    
    return checksum_map

def move_duplicates(checksum_map, top_folder):
    """Move duplicate files to a 'dup' folder, leaving only one copy."""
    dup_folder = os.path.join(top_folder, "dup")
    os.makedirs(dup_folder, exist_ok=True)

    for checksum, paths in checksum_map.items():
        if len(paths) > 1:
            # Keep the first file, move others
            for file_path in paths[1:]:
                try:
                    # Move the duplicate file
                    new_path = os.path.join(dup_folder, os.path.basename(file_path))
                    move(file_path, new_path)
                    print(f"Moved duplicate: {file_path} -> {new_path}")
                except Exception as e:
                    print(f"Error moving file {file_path}: {e}")

def display_duplicates(checksum_map):
    """Display duplicate files."""
    duplicates_found = False
    for checksum, paths in checksum_map.items():
        if len(paths) > 1:
            duplicates_found = True
            print(f"\nChecksum: {checksum}")
            for path in paths:
                print(f"  {path}")
    if not duplicates_found:
        print("No duplicate files found.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find duplicate files in a folder.")
    parser.add_argument("folder", help="Path to the folder to scan for duplicates.")
    parser.add_argument("--algorithm", default="md5", help="Checksum algorithm to use (default: md5).")
    parser.add_argument("--max-depth", type=int, default=None, help="Maximum depth to traverse (default: unlimited).")
    parser.add_argument("--extensions", nargs="+", default=None, help="Limit to files with these extensions (e.g., .pdf .jpg).")
    parser.add_argument("--max-size-mb", type=float, default=None, help="Maximum file size to process in MB (default: no limit).")
    
    args = parser.parse_args()

    folder = args.folder
    algorithm = args.algorithm
    max_depth = args.max_depth
    extensions = args.extensions
    max_size_mb = args.max_size_mb

    if extensions:
        extensions = [ext.lower() if ext.startswith('.') else f".{ext.lower()}" for ext in extensions]

    if os.path.isdir(folder):
        processor = FileProcessor()
        processor.start_progress_report()

        try:
            checksum_map = find_duplicates(folder, processor, extensions, max_depth, algorithm, max_size_mb)
        finally:
            processor.stop_signal = True  # Stop the progress report thread
            time.sleep(5)  # Allow the last progress message to print
        
        display_duplicates(checksum_map)
        move_duplicates(checksum_map, folder)
    else:
        print("Invalid folder path.")

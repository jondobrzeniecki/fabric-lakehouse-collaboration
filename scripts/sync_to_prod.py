#!/usr/bin/env python3
"""
Sync to Prod Script

This script reads a release manifest and determines which files should be
synced from the main branch to the prod branch. It supports dry-run mode
for previewing changes.

Usage:
    python sync_to_prod.py --manifest config/release-manifest.json [--dry-run]
"""

import argparse
import fnmatch
import json
import os
import subprocess
import sys
from pathlib import Path


def load_manifest(manifest_path: str) -> dict:
    """Load and parse the release manifest."""
    with open(manifest_path, 'r') as f:
        return json.load(f)


def get_all_files_in_path(base_path: str) -> list[str]:
    """Get all files within a directory path."""
    files = []
    path = Path(base_path)
    if path.is_file():
        return [str(path)]
    if path.is_dir():
        for file in path.rglob('*'):
            if file.is_file():
                files.append(str(file))
    return files


def should_exclude(file_path: str, never_include: list[str]) -> bool:
    """Check if a file matches any exclusion pattern."""
    for pattern in never_include:
        if fnmatch.fnmatch(file_path, pattern):
            return True
        # Also check if any parent directory matches
        if pattern.endswith('/'):
            if file_path.startswith(pattern) or f"/{pattern}" in f"/{file_path}":
                return True
    return False


def get_files_to_sync(manifest: dict) -> tuple[list[str], list[dict]]:
    """
    Determine which files should be synced based on the manifest.
    
    Returns:
        Tuple of (files_to_sync, summary_items)
    """
    files_to_sync = []
    summary = []
    never_include = manifest.get('neverInclude', [])
    
    # Process explicit items
    for item in manifest.get('items', []):
        path = item['path']
        include = item.get('include', True)
        comment = item.get('comment', '')
        
        if include:
            item_files = get_all_files_in_path(path)
            # Filter out excluded files
            item_files = [f for f in item_files if not should_exclude(f, never_include)]
            files_to_sync.extend(item_files)
            summary.append({
                'path': path,
                'status': '✅ INCLUDE',
                'files': len(item_files),
                'comment': comment
            })
        else:
            summary.append({
                'path': path,
                'status': '⏭️ SKIP',
                'files': 0,
                'comment': comment
            })
    
    # Process always-include items
    for path in manifest.get('alwaysInclude', []):
        item_files = get_all_files_in_path(path)
        item_files = [f for f in item_files if not should_exclude(f, never_include)]
        files_to_sync.extend(item_files)
        if item_files:
            summary.append({
                'path': path,
                'status': '📌 ALWAYS',
                'files': len(item_files),
                'comment': 'Always included'
            })
    
    # Remove duplicates while preserving order
    seen = set()
    unique_files = []
    for f in files_to_sync:
        if f not in seen:
            seen.add(f)
            unique_files.append(f)
    
    return unique_files, summary


def get_changed_files(files: list[str]) -> list[str]:
    """Compare files between main and prod branches to find changes."""
    changed = []
    
    for file_path in files:
        # Check if file exists in prod
        result = subprocess.run(
            ['git', 'cat-file', '-e', f'origin/prod:{file_path}'],
            capture_output=True
        )
        
        if result.returncode != 0:
            # File doesn't exist in prod
            changed.append(file_path)
        else:
            # File exists, check if content differs
            main_content = subprocess.run(
                ['git', 'show', f'main:{file_path}'],
                capture_output=True
            )
            prod_content = subprocess.run(
                ['git', 'show', f'origin/prod:{file_path}'],
                capture_output=True
            )
            
            if main_content.stdout != prod_content.stdout:
                changed.append(file_path)
    
    return changed


def main():
    parser = argparse.ArgumentParser(description='Sync files to prod based on manifest')
    parser.add_argument('--manifest', required=True, help='Path to release manifest JSON')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without applying')
    args = parser.parse_args()
    
    # Load manifest
    manifest = load_manifest(args.manifest)
    
    # Get files to sync
    files_to_sync, summary = get_files_to_sync(manifest)
    
    # Write summary
    with open('/tmp/sync_summary.txt', 'w') as f:
        for item in summary:
            f.write(f"{item['status']} {item['path']}\n")
            if item['comment']:
                f.write(f"   └─ {item['comment']}\n")
            if item['files'] > 0:
                f.write(f"   └─ {item['files']} file(s)\n")
    
    # Check for prod branch
    result = subprocess.run(
        ['git', 'ls-remote', '--heads', 'origin', 'prod'],
        capture_output=True,
        text=True
    )
    prod_exists = 'prod' in result.stdout
    
    if prod_exists:
        # Get only changed files
        changed_files = get_changed_files(files_to_sync)
    else:
        # All files are new if prod doesn't exist
        changed_files = files_to_sync
    
    # Write files to sync
    with open('/tmp/files_to_sync.txt', 'w') as f:
        f.write('\n'.join(changed_files))
    
    # Set outputs for GitHub Actions
    github_output = os.environ.get('GITHUB_OUTPUT')
    if github_output:
        with open(github_output, 'a') as f:
            if changed_files:
                f.write('has_changes=true\n')
                f.write('changed_files<<EOF\n')
                f.write('\n'.join(changed_files))
                f.write('\nEOF\n')
            else:
                f.write('has_changes=false\n')
    
    # Print summary
    print("\n" + "="*60)
    print("RELEASE MANIFEST SUMMARY")
    print("="*60)
    
    for item in summary:
        print(f"\n{item['status']} {item['path']}")
        if item['comment']:
            print(f"   └─ {item['comment']}")
        if item['files'] > 0:
            print(f"   └─ {item['files']} file(s)")
    
    print("\n" + "-"*60)
    print(f"Total files to sync: {len(files_to_sync)}")
    print(f"Files with changes:  {len(changed_files)}")
    print("-"*60)
    
    if changed_files:
        print("\nFiles to be synced:")
        for f in changed_files:
            print(f"  • {f}")
    else:
        print("\nNo changes detected - prod is up to date with main.")
    
    if args.dry_run:
        print("\n🔍 DRY RUN - No changes were applied")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())

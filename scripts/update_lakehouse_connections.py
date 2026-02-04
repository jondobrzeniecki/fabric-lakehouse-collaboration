#!/usr/bin/env python3
"""
Update Lakehouse Connections Script

This script updates lakehouse connection GUIDs in Fabric notebook files
to match the target environment configuration. It parses notebook-content.py
files and replaces workspace/lakehouse IDs based on environment variables.

Usage:
    python update_lakehouse_connections.py --workspace-id <guid> --lakehouse-id <guid>

Environment Variables:
    TARGET_WORKSPACE_ID - Target workspace GUID
    TARGET_LAKEHOUSE_ID - Target lakehouse GUID
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional


def parse_meta_block(content: str) -> tuple[str, dict, int, int]:
    """
    Extract the first META block from notebook content.
    
    Returns:
        Tuple of (meta_json_string, parsed_dict, start_index, end_index)
    """
    # Pattern to match the META block at the top of the notebook
    # The META block is formatted as comment lines starting with # META
    pattern = r'(# METADATA \*+\n\n)(# META \{[\s\S]*?# META \})\n'
    
    match = re.search(pattern, content)
    if not match:
        raise ValueError("Could not find META block in notebook content")
    
    meta_header = match.group(1)
    meta_block = match.group(2)
    start_idx = match.start()
    end_idx = match.end()
    
    # Parse the META block by removing "# META " prefixes and joining lines
    lines = meta_block.split('\n')
    json_lines = []
    for line in lines:
        if line.startswith('# META '):
            json_lines.append(line[7:])  # Remove "# META " prefix
        elif line.startswith('# META'):
            json_lines.append(line[6:])  # Handle lines like "# META}"
    
    json_str = '\n'.join(json_lines)
    
    try:
        meta_dict = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse META block as JSON: {e}\nContent:\n{json_str}")
    
    return meta_block, meta_dict, start_idx, end_idx


def format_meta_block(meta_dict: dict) -> str:
    """
    Format a dictionary back into the META block comment format.
    """
    json_str = json.dumps(meta_dict, indent=2)
    lines = json_str.split('\n')
    meta_lines = ['# META ' + line for line in lines]
    return '\n'.join(meta_lines)


def update_notebook_lakehouse(
    content: str,
    target_workspace_id: str,
    target_lakehouse_id: str,
    lakehouse_name: str = "lh1"
) -> tuple[str, bool]:
    """
    Update lakehouse connections in notebook content.
    
    Args:
        content: The notebook-content.py file content
        target_workspace_id: Target workspace GUID
        target_lakehouse_id: Target lakehouse GUID
        lakehouse_name: Name of the lakehouse (default: lh1)
    
    Returns:
        Tuple of (updated_content, was_modified)
    """
    try:
        meta_block, meta_dict, start_idx, end_idx = parse_meta_block(content)
    except ValueError as e:
        print(f"Warning: {e}")
        return content, False
    
    # Check if dependencies.lakehouse exists
    if 'dependencies' not in meta_dict or 'lakehouse' not in meta_dict['dependencies']:
        print("No lakehouse dependency found in notebook")
        return content, False
    
    lakehouse_config = meta_dict['dependencies']['lakehouse']
    original_lakehouse_id = lakehouse_config.get('default_lakehouse')
    original_workspace_id = lakehouse_config.get('default_lakehouse_workspace_id')
    
    # Check if update is needed
    if (original_lakehouse_id == target_lakehouse_id and 
        original_workspace_id == target_workspace_id):
        print(f"Lakehouse connection already up to date")
        return content, False
    
    print(f"Updating lakehouse connection:")
    print(f"  default_lakehouse: {original_lakehouse_id} -> {target_lakehouse_id}")
    print(f"  default_lakehouse_workspace_id: {original_workspace_id} -> {target_workspace_id}")
    
    # Update the lakehouse configuration
    lakehouse_config['default_lakehouse'] = target_lakehouse_id
    lakehouse_config['default_lakehouse_workspace_id'] = target_workspace_id
    
    # Update known_lakehouses
    if 'known_lakehouses' in lakehouse_config:
        for known_lh in lakehouse_config['known_lakehouses']:
            if known_lh.get('id') == original_lakehouse_id:
                known_lh['id'] = target_lakehouse_id
    
    # Format the updated META block
    new_meta_block = format_meta_block(meta_dict)
    
    # Get the content before and after the META block
    before_meta = content[:start_idx]
    after_meta = content[end_idx:]
    
    # Reconstruct the content
    updated_content = before_meta + '# METADATA ********************\n\n' + new_meta_block + '\n' + after_meta
    
    return updated_content, True


def update_pipeline_lakehouse(
    content: str,
    target_workspace_id: str,
    target_lakehouse_id: str
) -> tuple[str, bool]:
    """
    Update lakehouse connections in pipeline-content.json.
    
    Args:
        content: The pipeline-content.json file content
        target_workspace_id: Target workspace GUID
        target_lakehouse_id: Target lakehouse GUID
    
    Returns:
        Tuple of (updated_content, was_modified)
    """
    try:
        pipeline = json.loads(content)
    except json.JSONDecodeError as e:
        print(f"Failed to parse pipeline JSON: {e}")
        return content, False
    
    modified = False
    
    # Traverse activities to find lakehouse connections
    if 'properties' in pipeline and 'activities' in pipeline['properties']:
        for activity in pipeline['properties']['activities']:
            if 'typeProperties' not in activity:
                continue
            
            type_props = activity['typeProperties']
            
            # Check sink for lakehouse connection
            if 'sink' in type_props and 'datasetSettings' in type_props['sink']:
                sink_settings = type_props['sink']['datasetSettings']
                if 'connectionSettings' in sink_settings:
                    conn_settings = sink_settings['connectionSettings']
                    if 'properties' in conn_settings and 'typeProperties' in conn_settings['properties']:
                        type_props_inner = conn_settings['properties']['typeProperties']
                        
                        old_artifact_id = type_props_inner.get('artifactId')
                        old_workspace_id = type_props_inner.get('workspaceId')
                        
                        if old_artifact_id or old_workspace_id:
                            print(f"Updating pipeline sink connection:")
                            if old_artifact_id:
                                print(f"  artifactId: {old_artifact_id} -> {target_lakehouse_id}")
                                type_props_inner['artifactId'] = target_lakehouse_id
                                modified = True
                            if old_workspace_id:
                                print(f"  workspaceId: {old_workspace_id} -> {target_workspace_id}")
                                type_props_inner['workspaceId'] = target_workspace_id
                                modified = True
    
    if modified:
        return json.dumps(pipeline, indent=2), True
    
    return content, False


def find_notebooks(root_dir: Path, changed_files: list[str] | None = None) -> list[Path]:
    """Find all notebook-content.py files in Notebook folders."""
    notebooks = []
    for folder in root_dir.iterdir():
        if folder.is_dir() and folder.name.endswith('.Notebook'):
            notebook_file = folder / 'notebook-content.py'
            if notebook_file.exists():
                # If changed_files is provided, only include if this notebook was changed
                if changed_files is not None:
                    folder_prefix = str(folder.relative_to(root_dir.parent))
                    if not any(cf.startswith(folder_prefix) for cf in changed_files):
                        continue
                notebooks.append(notebook_file)
    return notebooks


def find_pipelines(root_dir: Path, changed_files: list[str] | None = None) -> list[Path]:
    """Find all pipeline-content.json files in DataPipeline folders."""
    pipelines = []
    for folder in root_dir.iterdir():
        if folder.is_dir() and folder.name.endswith('.DataPipeline'):
            pipeline_file = folder / 'pipeline-content.json'
            if pipeline_file.exists():
                # If changed_files is provided, only include if this pipeline was changed
                if changed_files is not None:
                    folder_prefix = str(folder.relative_to(root_dir.parent))
                    if not any(cf.startswith(folder_prefix) for cf in changed_files):
                        continue
                pipelines.append(pipeline_file)
    return pipelines


def main():
    parser = argparse.ArgumentParser(
        description='Update lakehouse connections in Fabric notebooks and pipelines'
    )
    parser.add_argument(
        '--workspace-id',
        default=os.environ.get('TARGET_WORKSPACE_ID'),
        help='Target workspace GUID (or set TARGET_WORKSPACE_ID env var)'
    )
    parser.add_argument(
        '--lakehouse-id',
        default=os.environ.get('TARGET_LAKEHOUSE_ID'),
        help='Target lakehouse GUID (or set TARGET_LAKEHOUSE_ID env var)'
    )
    parser.add_argument(
        '--root-dir',
        default='fabric-lakehouse-collaboration',
        help='Root directory containing Fabric artifacts'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be changed without making changes'
    )
    parser.add_argument(
        '--changed-files',
        type=str,
        default=os.environ.get('CHANGED_FILES'),
        help='Comma-separated list of changed files (filters to only update these)'
    )
    parser.add_argument(
        '--changed-files-file',
        type=str,
        default=None,
        help='File containing list of changed files (one per line)'
    )
    
    args = parser.parse_args()
    
    if not args.workspace_id or not args.lakehouse_id:
        print("Error: Both workspace-id and lakehouse-id are required")
        print("Set via arguments or environment variables TARGET_WORKSPACE_ID and TARGET_LAKEHOUSE_ID")
        sys.exit(1)
    
    # Validate GUIDs
    guid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
    if not guid_pattern.match(args.workspace_id):
        print(f"Error: Invalid workspace-id format: {args.workspace_id}")
        sys.exit(1)
    if not guid_pattern.match(args.lakehouse_id):
        print(f"Error: Invalid lakehouse-id format: {args.lakehouse_id}")
        sys.exit(1)
    
    root_dir = Path(args.root_dir)
    if not root_dir.exists():
        print(f"Error: Root directory not found: {root_dir}")
        sys.exit(1)
    
    # Parse changed files if provided
    changed_files = None
    if args.changed_files:
        changed_files = [f.strip() for f in args.changed_files.split(',') if f.strip()]
        print(f"Filtering to {len(changed_files)} changed file(s)")
    elif args.changed_files_file:
        try:
            with open(args.changed_files_file, 'r') as f:
                changed_files = [line.strip() for line in f if line.strip()]
            print(f"Filtering to {len(changed_files)} changed file(s) from {args.changed_files_file}")
        except FileNotFoundError:
            print(f"Warning: Changed files file not found: {args.changed_files_file}")
    
    print(f"Target Workspace ID: {args.workspace_id}")
    print(f"Target Lakehouse ID: {args.lakehouse_id}")
    print(f"Root Directory: {root_dir.absolute()}")
    print(f"Dry Run: {args.dry_run}")
    print()
    
    total_modified = 0
    
    # Process notebooks
    notebooks = find_notebooks(root_dir, changed_files)
    print(f"Found {len(notebooks)} notebook(s)")
    
    for notebook_path in notebooks:
        print(f"\nProcessing: {notebook_path}")
        content = notebook_path.read_text(encoding='utf-8')
        updated_content, was_modified = update_notebook_lakehouse(
            content, args.workspace_id, args.lakehouse_id
        )
        
        if was_modified:
            total_modified += 1
            if not args.dry_run:
                notebook_path.write_text(updated_content, encoding='utf-8')
                print(f"  ✓ Updated")
            else:
                print(f"  Would update (dry-run)")
    
    # Process pipelines
    pipelines = find_pipelines(root_dir, changed_files)
    print(f"\nFound {len(pipelines)} pipeline(s)")
    
    for pipeline_path in pipelines:
        print(f"\nProcessing: {pipeline_path}")
        content = pipeline_path.read_text(encoding='utf-8')
        updated_content, was_modified = update_pipeline_lakehouse(
            content, args.workspace_id, args.lakehouse_id
        )
        
        if was_modified:
            total_modified += 1
            if not args.dry_run:
                pipeline_path.write_text(updated_content, encoding='utf-8')
                print(f"  ✓ Updated")
            else:
                print(f"  Would update (dry-run)")
    
    print(f"\n{'=' * 50}")
    print(f"Total files modified: {total_modified}")
    
    if args.dry_run and total_modified > 0:
        print("\nNote: No changes were made (dry-run mode)")
    
    return 0 if total_modified >= 0 else 1


if __name__ == '__main__':
    sys.exit(main())

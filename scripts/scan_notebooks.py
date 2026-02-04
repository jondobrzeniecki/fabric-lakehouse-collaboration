#!/usr/bin/env python3
"""
Scan Notebooks Script

This script scans all Fabric notebooks in the repository and outputs
their current lakehouse connection settings in a format suitable for
GitHub Actions comments.

Usage:
    python scan_notebooks.py [--root-dir <path>] [--format markdown|json]
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path


def parse_meta_block(content: str) -> dict | None:
    """
    Extract the first META block from notebook content.
    
    Returns:
        Parsed dict or None if not found
    """
    pattern = r'(# METADATA \*+\n\n)(# META \{[\s\S]*?# META \})\n'
    
    match = re.search(pattern, content)
    if not match:
        return None
    
    meta_block = match.group(2)
    
    # Parse the META block by removing "# META " prefixes
    lines = meta_block.split('\n')
    json_lines = []
    for line in lines:
        if line.startswith('# META '):
            json_lines.append(line[7:])
        elif line.startswith('# META'):
            json_lines.append(line[6:])
    
    json_str = '\n'.join(json_lines)
    
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return None


def scan_notebook(notebook_path: Path) -> dict | None:
    """
    Scan a single notebook and extract lakehouse info.
    
    Returns:
        Dict with notebook info or None if no lakehouse found
    """
    content = notebook_path.read_text(encoding='utf-8')
    meta = parse_meta_block(content)
    
    if not meta:
        return None
    
    if 'dependencies' not in meta or 'lakehouse' not in meta['dependencies']:
        return None
    
    lh = meta['dependencies']['lakehouse']
    
    return {
        'notebook_name': notebook_path.parent.name.replace('.Notebook', ''),
        'notebook_path': str(notebook_path),
        'default_lakehouse': lh.get('default_lakehouse', 'N/A'),
        'default_lakehouse_name': lh.get('default_lakehouse_name', 'N/A'),
        'default_lakehouse_workspace_id': lh.get('default_lakehouse_workspace_id', 'N/A'),
        'known_lakehouses': [kl.get('id') for kl in lh.get('known_lakehouses', [])]
    }


def scan_pipeline(pipeline_path: Path) -> dict | None:
    """
    Scan a pipeline and extract lakehouse connection info.
    
    Returns:
        Dict with pipeline info or None if no lakehouse found
    """
    content = pipeline_path.read_text(encoding='utf-8')
    
    try:
        pipeline = json.loads(content)
    except json.JSONDecodeError:
        return None
    
    # Look for lakehouse connections in activities
    lakehouse_refs = []
    
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
                        if 'artifactId' in type_props_inner or 'workspaceId' in type_props_inner:
                            lakehouse_refs.append({
                                'activity': activity.get('name', 'Unknown'),
                                'artifactId': type_props_inner.get('artifactId', 'N/A'),
                                'workspaceId': type_props_inner.get('workspaceId', 'N/A')
                            })
    
    if not lakehouse_refs:
        return None
    
    return {
        'pipeline_name': pipeline_path.parent.name.replace('.DataPipeline', ''),
        'pipeline_path': str(pipeline_path),
        'lakehouse_refs': lakehouse_refs
    }


def find_notebooks(root_dir: Path, changed_files: list[str] | None = None) -> list[Path]:
    """Find all notebook-content.py files, optionally filtered by changed files."""
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
    return sorted(notebooks)


def find_pipelines(root_dir: Path, changed_files: list[str] | None = None) -> list[Path]:
    """Find all pipeline-content.json files, optionally filtered by changed files."""
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
    return sorted(pipelines)


def format_markdown(notebooks: list[dict], pipelines: list[dict]) -> str:
    """Format scan results as markdown for PR comment."""
    lines = []
    
    lines.append("## 📓 Notebooks with Lakehouse Connections\n")
    
    if notebooks:
        lines.append("| Notebook | Lakehouse Name | Lakehouse ID | Workspace ID |")
        lines.append("|----------|---------------|--------------|--------------|")
        for nb in notebooks:
            lines.append(f"| `{nb['notebook_name']}` | {nb['default_lakehouse_name']} | `{nb['default_lakehouse'][:8]}...` | `{nb['default_lakehouse_workspace_id'][:8]}...` |")
        lines.append("")
    else:
        lines.append("*No notebooks with lakehouse connections found.*\n")
    
    lines.append("## 🔗 Pipelines with Lakehouse Connections\n")
    
    if pipelines:
        lines.append("| Pipeline | Activity | Artifact ID | Workspace ID |")
        lines.append("|----------|----------|-------------|--------------|")
        for pl in pipelines:
            for ref in pl['lakehouse_refs']:
                artifact_display = f"`{ref['artifactId'][:8]}...`" if ref['artifactId'] != 'N/A' else 'N/A'
                workspace_display = f"`{ref['workspaceId'][:8]}...`" if ref['workspaceId'] != 'N/A' and ref['workspaceId'] != '00000000-0000-0000-0000-000000000000' else 'N/A'
                lines.append(f"| `{pl['pipeline_name']}` | {ref['activity']} | {artifact_display} | {workspace_display} |")
        lines.append("")
    else:
        lines.append("*No pipelines with lakehouse connections found.*\n")
    
    # Add detailed info section (collapsed)
    lines.append("<details>")
    lines.append("<summary>📋 Full Connection Details</summary>\n")
    
    for nb in notebooks:
        lines.append(f"### {nb['notebook_name']}")
        lines.append(f"- **Lakehouse Name**: `{nb['default_lakehouse_name']}`")
        lines.append(f"- **Lakehouse ID**: `{nb['default_lakehouse']}`")
        lines.append(f"- **Workspace ID**: `{nb['default_lakehouse_workspace_id']}`")
        lines.append("")
    
    for pl in pipelines:
        lines.append(f"### {pl['pipeline_name']}")
        for ref in pl['lakehouse_refs']:
            lines.append(f"- **Activity**: {ref['activity']}")
            lines.append(f"  - Artifact ID: `{ref['artifactId']}`")
            lines.append(f"  - Workspace ID: `{ref['workspaceId']}`")
        lines.append("")
    
    lines.append("</details>\n")
    
    # Add instructions
    lines.append("---\n")
    lines.append("### 🔄 Update Lakehouse Connections\n")
    lines.append("To update all notebooks and pipelines to use a different lakehouse, comment on this PR with:\n")
    lines.append("```")
    lines.append("/update-lakehouse <workspace-id> <lakehouse-id>")
    lines.append("```\n")
    lines.append("**Example:**")
    lines.append("```")
    lines.append("/update-lakehouse 50256700-cd75-43bf-ae56-1f0a129e7f0b 4c9ba57b-f0a6-4e1e-a6ff-021eb70fc836")
    lines.append("```\n")
    lines.append("*The workflow will update all connection GUIDs and commit the changes to this PR.*")
    
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description='Scan Fabric notebooks for lakehouse connections')
    parser.add_argument('--root-dir', default='fabric-lakehouse-collaboration',
                        help='Root directory containing Fabric artifacts')
    parser.add_argument('--format', choices=['markdown', 'json'], default='markdown',
                        help='Output format')
    parser.add_argument('--changed-files', type=str, default=None,
                        help='Comma-separated list of changed files (filters to only scan these)')
    parser.add_argument('--changed-files-file', type=str, default=None,
                        help='File containing list of changed files (one per line)')
    
    args = parser.parse_args()
    
    root_dir = Path(args.root_dir)
    if not root_dir.exists():
        print(f"Error: Root directory not found: {root_dir}", file=sys.stderr)
        sys.exit(1)
    
    # Parse changed files if provided
    changed_files = None
    if args.changed_files:
        changed_files = [f.strip() for f in args.changed_files.split(',') if f.strip()]
    elif args.changed_files_file:
        try:
            with open(args.changed_files_file, 'r') as f:
                changed_files = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            print(f"Warning: Changed files file not found: {args.changed_files_file}", file=sys.stderr)
    
    # Scan notebooks
    notebook_paths = find_notebooks(root_dir, changed_files)
    notebooks = []
    for path in notebook_paths:
        info = scan_notebook(path)
        if info:
            notebooks.append(info)
    
    # Scan pipelines
    pipeline_paths = find_pipelines(root_dir, changed_files)
    pipelines = []
    for path in pipeline_paths:
        info = scan_pipeline(path)
        if info:
            pipelines.append(info)
    
    # Output results
    if args.format == 'json':
        result = {
            'notebooks': notebooks,
            'pipelines': pipelines
        }
        print(json.dumps(result, indent=2))
    else:
        print(format_markdown(notebooks, pipelines))
    
    return 0


if __name__ == '__main__':
    sys.exit(main())

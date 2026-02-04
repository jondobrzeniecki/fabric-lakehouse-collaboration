# Fabric Lakehouse Collaboration

This repository contains Microsoft Fabric workspace artifacts with automated CI/CD for lakehouse connection management.

## Overview

When working with Fabric Git integration, notebooks and pipelines contain environment-specific GUIDs for lakehouse connections. This repository includes a GitHub Actions workflow that helps you update these connections when creating pull requests.

## CI/CD Pipeline

### How It Works

The workflow operates in two phases:

**Phase 1: Scan (Automatic)**

When you create or update a pull request that modifies notebooks or pipelines:

1. The workflow scans **only the changed files** in your PR
2. It posts a comment listing all notebooks/pipelines with their current lakehouse connections
3. It provides instructions for updating the connections

**Phase 2: Update (User-Triggered)**

To update the lakehouse connections, comment on the PR with:

```
/update-lakehouse <workspace-id> <lakehouse-id>
```

**Example:**
```
/update-lakehouse 50256700-cd75-43bf-ae56-1f0a129e7f0b 4c9ba57b-f0a6-4e1e-a6ff-021eb70fc836
```

The workflow will then:
1. Update only the notebooks/pipelines that were changed in the PR
2. Replace `default_lakehouse` and `default_lakehouse_workspace_id` in notebook META blocks
3. Replace `artifactId` and `workspaceId` in pipeline connection settings
4. Commit and push the changes to your PR branch
5. Post a confirmation comment with the list of updated files

### Finding Your Workspace and Lakehouse IDs

You can find these GUIDs in the Fabric portal URLs:

- **Workspace ID**: Go to your workspace, the URL contains `.../groups/<workspace-id>/...`
- **Lakehouse ID**: Open your lakehouse, the URL contains `.../lakehouses/<lakehouse-id>...`

### Running Locally

You can test the update script locally:

```bash
# Set environment variables
export TARGET_WORKSPACE_ID="your-workspace-guid"
export TARGET_LAKEHOUSE_ID="your-lakehouse-guid"

# Dry run (see what would change)
python scripts/update_lakehouse_connections.py --dry-run

# Apply changes
python scripts/update_lakehouse_connections.py

# Update only specific files
python scripts/update_lakehouse_connections.py --changed-files "fabric-lakehouse-collaboration/MyNotebook.Notebook/notebook-content.py"
```

## Repository Structure

```
fabric-lakehouse-collaboration/
├── .github/
│   └── workflows/
│       └── update-lakehouse-connections.yml  # CI/CD workflow
├── scripts/
│   └── update_lakehouse_connections.py       # Updates lakehouse GUIDs
├── fabric-lakehouse-collaboration/
│   ├── config/
│   │   └── environments.template.json        # Environment config template
│   ├── *.Notebook/
│   │   ├── .platform                         # Artifact metadata
│   │   └── notebook-content.py               # Notebook code + META block
│   ├── *.Lakehouse/
│   │   ├── .platform
│   │   ├── lakehouse.metadata.json
│   │   └── shortcuts.metadata.json
│   └── *.DataPipeline/
│       ├── .platform
│       └── pipeline-content.json
└── .gitignore
```

## Adding New Environments

To add additional environments (e.g., test, prod):

1. Create new branches (e.g., `release/test`, `release/prod`)
2. Add corresponding secrets:
   - `TEST_WORKSPACE_ID`, `TEST_LAKEHOUSE_ID`
   - `PROD_WORKSPACE_ID`, `PROD_LAKEHOUSE_ID`
3. Update the workflow to handle additional branches

## Troubleshooting

### No changes detected
- The lakehouse connections may already match the target environment
- Check if the notebook files contain the expected META block structure

### Workflow doesn't run
- Ensure the PR modifies files in the `fabric-lakehouse-collaboration/` folder
- Check that the secrets `DEV_WORKSPACE_ID` and `DEV_LAKEHOUSE_ID` are configured

## References

- [Microsoft Fabric Git Integration](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/intro-to-git-integration)
- [Manage Deployment with Git](https://learn.microsoft.com/en-us/fabric/cicd/manage-deployment)
- [Fabric REST API](https://learn.microsoft.com/en-us/rest/api/fabric/)
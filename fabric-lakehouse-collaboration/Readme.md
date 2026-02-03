# Fabric Lakehouse Collaboration

This repository contains Microsoft Fabric workspace artifacts with automated CI/CD for lakehouse connection management.

## Overview

When working with Fabric Git integration, notebooks and pipelines contain environment-specific GUIDs for lakehouse connections. This repository includes a GitHub Actions workflow that automatically updates these connections when creating pull requests.

## CI/CD Pipeline

### How It Works

When you create a pull request to `main` (dev environment), the workflow:

1. **Validates** the target lakehouse exists using the Fabric REST API
2. **Updates** lakehouse connection GUIDs in:
   - `notebook-content.py` files (META block with `default_lakehouse`, `default_lakehouse_workspace_id`)
   - `pipeline-content.json` files (`artifactId`, `workspaceId` in connection settings)
3. **Commits** the changes back to your PR branch
4. **Comments** on the PR with a summary of changes

### Required GitHub Secrets

Configure these secrets in your repository settings (**Settings → Secrets and variables → Actions**):

| Secret | Description | How to Obtain |
|--------|-------------|---------------|
| `DEV_WORKSPACE_ID` | Target Fabric workspace GUID | Fabric Portal → Workspace Settings → URL contains the ID |
| `DEV_LAKEHOUSE_ID` | Target lakehouse GUID | Fabric Portal → Lakehouse → URL contains the ID |

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
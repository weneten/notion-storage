# Storage Project

A Flask application for file storage using Notion as a backend.

## Deploying to Koyeb

### Prerequisites

1. A [Koyeb account](https://app.koyeb.com)
2. Your project repository on GitHub, GitLab, or Bitbucket

### Deployment Steps

#### Option 1: Using Koyeb Web Interface

1. Log in to your [Koyeb account](https://app.koyeb.com)
2. Click on "Create App"
3. Select "GitHub" as the deployment method
4. Choose your repository and branch
5. Set the following:
   - Name: storage-project (or your preferred name)
   - Type: Web Service
   - Runtime: Docker
   - Port: 8080
6. Set your environment variables:
   - `NOTION_API_TOKEN`: Your Notion API token
   - `NOTION_USER_DB_ID`: Your Notion user database ID
   - `GLOBAL_FILE_INDEX_DB_ID`: Your Notion global file index database ID
   - `SECRET_KEY`: A secure random string for Flask's session encryption
7. Click "Deploy"

#### Option 2: Using Koyeb CLI

1. Install the [Koyeb CLI](https://www.koyeb.com/docs/cli/installation)
2. Log in to Koyeb:
   ```
   koyeb login
   ```
3. Deploy your app:
   ```
   koyeb app init storage-project \
     --git github.com/yourusername/storage-project \
     --git-branch main \
     --type web \
     --port 8080 \
     --env NOTION_API_TOKEN=your_notion_api_token \
     --env NOTION_USER_DB_ID=your_notion_user_db_id \
     --env GLOBAL_FILE_INDEX_DB_ID=your_global_file_index_db_id \
     --env SECRET_KEY=your_secret_key
   ```

#### Option 3: Using koyeb.yaml (Recommended)

1. Update the `koyeb.yaml` file in your repository with your actual repository URL
2. Add your environment variables in the Koyeb dashboard after deployment
3. Deploy using the CLI:
   ```
   koyeb app create --name storage-project --service web
   ```

## Environment Variables

The following environment variables need to be configured:

- `NOTION_API_TOKEN`: Your Notion API token
- `NOTION_USER_DB_ID`: Your Notion user database ID 
- `GLOBAL_FILE_INDEX_DB_ID`: Your Notion global file index database ID
- `SECRET_KEY`: Secret key for Flask (will be auto-generated if not provided)
- `PORT`: Port to run the application (defaults to 8080)

## Local Development

To run the app locally:

```bash
pip install -r requirements.txt
export FLASK_ENV=development
python app.py
```

The app will be available at http://localhost:8080 
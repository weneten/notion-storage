# Storage Project

This project is a Flask-based file storage application that integrates with Notion for user authentication and file storage.

## Docker Setup

### Prerequisites
- Docker installed on your system
- Docker Compose installed on your system
- Environment variables set (see Configuration section)

### Building and Running with Docker

1. **Build and start the container**:
   ```bash
   docker-compose up -d --build
   ```

2. **Stop the container**:
   ```bash
   docker-compose down
   ```

### Configuration

Create a `.env` file in the project root with the following environment variables:

```
SECRET_KEY=your_secret_key
NOTION_API_TOKEN=your_notion_api_token
NOTION_USER_DB_ID=your_notion_user_database_id
GLOBAL_FILE_INDEX_DB_ID=your_global_file_index_database_id
```

### Building the Docker Image Manually

If you prefer to build and run without docker-compose:

```bash
# Build the image
docker build -t storage-project .

# Run the container
docker run -p 5000:5000 --env-file .env storage-project
```

## Development

For local development without Docker, refer to the Procfile:

```bash
pip install -r requirements.txt
gunicorn app:app --worker-class eventlet -b 0.0.0.0:5000
``` 
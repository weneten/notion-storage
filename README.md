# Storage Project

A Flask application for file storage using Notion as a backend database.

## Back4app Deployment Instructions

### Prerequisites
- Back4app account (https://back4app.com)
- Back4app CLI installed
- Docker installed locally (for testing)
- Notion API token and database IDs

### Environment Variables
Before deploying, make sure to set up the following environment variables in Back4app:

- `NOTION_API_TOKEN`: Your Notion API token
- `NOTION_USER_DB_ID`: Notion User Database ID
- `GLOBAL_FILE_INDEX_DB_ID`: Notion Global File Index Database ID
- `SECRET_KEY`: Secret key for Flask application security

### Local Testing with Docker

1. Create a `.env` file with your environment variables:
```
NOTION_API_TOKEN=your_token
NOTION_USER_DB_ID=your_user_db_id
GLOBAL_FILE_INDEX_DB_ID=your_file_index_db_id
SECRET_KEY=your_secret_key
```

2. Build and run the Docker container locally:
```
docker-compose up --build
```

3. Access the application at http://localhost:8080

### Deploying to Back4app

1. Log in to Back4app CLI:
```
b4a login
```

2. Deploy the application:
```
b4a deploy
```

3. Set your environment variables in the Back4app dashboard.

4. Access your application using the URL provided by Back4app.

## Application Features
- User authentication
- File upload and storage in Notion
- File download functionality
- File sharing capabilities
- WebSocket notifications 
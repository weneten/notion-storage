FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Expose the port that the app will run on
ENV PORT=5000
EXPOSE 5000

# Command to run the app using gunicorn as specified in Procfile
CMD gunicorn app:app --worker-class eventlet -b 0.0.0.0:$PORT 
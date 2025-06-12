# Start from a lightweight Python image
FROM python:3.10-slim

# Set work directory
WORKDIR /app

# Install system dependencies (if needed)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      libevent-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip setuptools
RUN pip install -r requirements.txt

# Copy application code
COPY . .

# Expose the port (Cloud Run/GCP uses 8080 by default, but ENV sets the right $PORT)
EXPOSE 8080

# Default command to run the app with Gunicorn + Eventlet
CMD ["gunicorn", "-b", "0.0.0.0:8080", "-k", "eventlet", "-w", "1", "--timeout", "60", "app:app"]

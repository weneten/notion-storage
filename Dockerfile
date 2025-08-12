# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set working directory in the container
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# Qoddi expects the application to listen on the PORT environment variable.
# Set a default that matches Qoddi's standard port while still allowing an
# override at runtime.
ENV PORT=8080

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application
COPY . .

# Create necessary directories
RUN mkdir -p /app/uploads /app/static /app/templates

# Set proper permissions
RUN chmod -R 755 /app

# Expose the port the app runs on for Qoddi
EXPOSE 8080

# Health check that respects the runtime port
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:$PORT/ || exit 1

# Run the application using gunicorn with eventlet worker
# Use a shell form to allow environment variable expansion for the port.
CMD ["sh", "-c", "gunicorn app:app --worker-class eventlet --workers 1 --bind 0.0.0.0:${PORT:-8080} --timeout 300 --keep-alive 2"]

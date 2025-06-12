FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Expose port
ENV PORT=8080
EXPOSE 8080

# Start the application
CMD gunicorn app:app --worker-class eventlet -b 0.0.0.0:${PORT} 
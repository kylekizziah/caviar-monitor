FROM python:3.11-slim

# Avoid python buffering in logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install runtime deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app
COPY . .

# Default command can be overridden in Render "Docker Command"
CMD ["python", "-m", "email_digest"]

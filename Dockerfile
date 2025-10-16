FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install MySQL/MariaDB drivers
RUN pip install --no-cache-dir pymysql mysqlclient

# Copy the application code
COPY . .

# Default command (can be overridden in docker-compose)
CMD ["python", "src/main.py"]

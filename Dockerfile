# Use a lightweight and stable Python 3.11 base image
FROM python:3.11-slim-buster AS base

# Set environment variables for production
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/home/user/.local/bin:$PATH" 
# Add a non-root user for security
RUN useradd -m -u 1000 user

# Switch to non-root user
USER user

# Set working directory
WORKDIR /app

COPY app.py .

# Install dependencies
COPY --chown=user ./requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --upgrade -r requirements.txt

# Copy application files
COPY --chown=user . /app

# Expose the application port
EXPOSE 8004

# Command to run the application
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8004", "--workers", "4", "--log-level", "info"]
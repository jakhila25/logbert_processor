FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
# Expose the application port
EXPOSE 8001
CMD ["uviron", "app:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "4"]

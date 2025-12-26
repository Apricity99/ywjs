FROM python:3.11-slim

RUN pip install --no-cache-dir fastapi uvicorn[standard] pydantic kubernetes requests

WORKDIR /app
COPY main.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

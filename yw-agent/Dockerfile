FROM python:3.11-slim

RUN pip install psutil requests

WORKDIR /app
COPY main.py .

CMD ["python", "main.py"]

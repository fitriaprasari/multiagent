FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
ENV BETA=5 GAMMA=0.3 DELTA_MS=10
EXPOSE 5000
CMD ["python", "app.py"]

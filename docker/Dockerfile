# ============================================================
# Dockerfile
# Goal: Containerize the stream producer
# Author: Esra Demirturk Duman
# ============================================================

# Neden Docker?
# stream_producer.py'yi her ortamda aynı şekilde çalıştırmak için.
# "Bende çalışıyor" problemini ortadan kaldırır.
# Gerçek hayatta Kubernetes'te veya Azure Container Instance'da koşar.

# Base image
# Neden python:3.10-slim: Küçük boyutlu, production için uygun
FROM python:3.10-slim

# Working directory
WORKDIR /app

# Install dependencies
# Neden önce requirements: Docker cache'i verimli kullanır
# Kod değişse bile requirements değişmediyse bu layer cache'den gelir
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source files
COPY stream_producer.py .
COPY hotel_raw_stream.csv .

# Environment variables (default values)
# Gerçek değerler docker run sırasında -e ile geçilir
ENV STREAM_FILE=hotel_raw_stream.csv
ENV EVENTSTREAM_URL=""
ENV DELAY_SECONDS=0.5
ENV BATCH_SIZE=1

# Run the producer
CMD ["python", "stream_producer.py"]

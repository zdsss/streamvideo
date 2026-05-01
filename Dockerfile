FROM python:3.12-slim

# Install system deps + tini (PID 1 for zombie reaping)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Install streamlink and yt-dlp
RUN pip install --no-cache-dir streamlink yt-dlp

WORKDIR /app

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright (optional, for Douyin fallback)
RUN pip install --no-cache-dir playwright && playwright install --with-deps chromium || true

# Copy project files — MUST include streamvideo/ package
COPY server.py ./
COPY streamvideo/ ./streamvideo/
COPY static/ ./static/

# Data directory
RUN mkdir -p recordings

# Non-root user
RUN useradd -m -r streamvideo && chown -R streamvideo:streamvideo /app
USER streamvideo

EXPOSE 8080

ENV PYTHONUNBUFFERED=1

# Use Python for healthcheck (curl not available in slim image)
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/api/health')" || exit 1

# tini as PID 1, then python server
ENTRYPOINT ["tini", "--"]
CMD ["python", "server.py"]

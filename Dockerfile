FROM python:3.12-slim

# 系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# 安装 streamlink 和 yt-dlp
RUN pip install --no-cache-dir streamlink yt-dlp

WORKDIR /app

# Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 安装 Playwright（可选，用于抖音 fallback）
RUN pip install --no-cache-dir playwright && playwright install --with-deps chromium || true

# 复制项目文件
COPY server.py recorder.py database.py auth.py quota.py distribute.py \
     highlight.py danmaku.py clipgen.py subtitle_gen.py cover_gen.py ./
COPY static/ ./static/

# 数据目录
RUN mkdir -p recordings

EXPOSE 8080

# 环境变量
ENV PYTHONUNBUFFERED=1

HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD curl -f http://localhost:8080/api/health || exit 1

CMD ["python", "server.py"]

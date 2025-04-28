FROM python:3.11-slim

# 環境変数
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Bangkok

WORKDIR /app

# 依存インストール
COPY requirements.txt .
RUN pip install --no-color --disable-pip-version-check -r requirements.txt

# アプリケーションコード
COPY . .
VOLUME ["/app/data"]

CMD ["python", "fetcher.py"]

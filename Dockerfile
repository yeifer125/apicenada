# ---------- Imagen base con Python ----------
FROM python:3.10-slim

# ---------- Variables de entorno ----------
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# ---------- Instalar dependencias del sistema ----------
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    fonts-liberation \
    fonts-unifont \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libgtk-3-0 \
    libnss3 \
    libnspr4 \
    libxshmfence1 \
    && rm -rf /var/lib/apt/lists/*

# ---------- Instalar dependencias Python ----------
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------- Instalar navegadores Playwright ----------
RUN python -m playwright install chromium

# ---------- Copiar código ----------
COPY . .

# ---------- Exponer puerto ----------
EXPOSE 5000

# ---------- Comando de ejecución ----------
CMD ["python", "main.py"]

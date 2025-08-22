# Base Python 3.11 slim
FROM python:3.11-slim

# ---------------- Dependencias del sistema para Chromium ----------------
RUN apt-get update && apt-get install -y \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 \
    libgbm1 libasound2 libpangocairo-1.0-0 libpango-1.0-0 libx11-xcb1 \
    libxfixes3 libxrender1 libxext6 libx11-6 libxi6 \
    wget curl ca-certificates fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# ---------------- Directorio de trabajo ----------------
WORKDIR /app

# ---------------- Copiar requirements e instalar dependencias Python ----------------
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# ---------------- Instalar Playwright y navegador Chromium ----------------
RUN pip install --no-cache-dir playwright
RUN playwright install chromium

# ---------------- Copiar todo el código ----------------
COPY . /app

# ---------------- Exponer puerto Flask ----------------
EXPOSE 5000

# ---------------- Ejecutar app ----------------
CMD ["python", "main.py"]

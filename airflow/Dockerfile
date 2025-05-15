FROM apache/airflow:2.10.2-python3.11

USER root

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    curl \
    unzip \
    xdg-utils \ 
    fonts-liberation \
    libnss3 \
    libxss1 \
    libasound2 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libcurl4 \
    libdbus-1-3 \
    libexpat1 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libudev1 \
    libvulkan1 \
    libx11-6 \
    libxcb1 \
    libxext6 \
    libxfixes3 \
    libxkbcommon0 \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome and ChromeDriver
RUN apt-get update && \
    apt-get install -y wget unzip curl gnupg --no-install-recommends && \
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb && \
    CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+') && \
    CHROMEDRIVER_VERSION=$(curl -sS "https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json" | grep -A10 "$CHROME_VERSION" | grep 'linux64' | grep 'chromedriver' | grep -oP 'https://[^"]+') && \
    curl -sSL "$CHROMEDRIVER_VERSION" -o chromedriver_linux64.zip && \
    unzip chromedriver_linux64.zip && \
    mv chromedriver /usr/bin/chromedriver && \
    chmod +x /usr/bin/chromedriver && \
    rm chromedriver_linux64.zip

USER airflow

# Install Python deps
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

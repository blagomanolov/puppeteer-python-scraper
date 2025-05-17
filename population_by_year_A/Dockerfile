# Base image with Python and Node.js
FROM node:20-bullseye

# Set working directory
WORKDIR /app

# Install Python and Chromium for Puppeteer
RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    chromium \
    libglib2.0-0 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxi6 \
    libxtst6 \
    libxrandr2 \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    cron \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set environment variable for Puppeteer
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

# Copy all files
COPY scraper/ ./scraper
COPY parser/ ./parser
COPY requirements.txt ./
COPY crontab.txt /etc/cron.d/auto-task

# Install Node.js dependencies
WORKDIR /app/scraper
RUN npm install

# Install Python dependencies
WORKDIR /app
RUN pip3 install --no-cache-dir -r requirements.txt

# Make the cron job executable and register it
RUN chmod 0644 /etc/cron.d/auto-task && crontab /etc/cron.d/auto-task

# Create cron log path
RUN mkdir -p /var/log/cron && touch /var/log/cron/cron.log

# Start script
RUN echo '#!/bin/bash\nnode /app/scraper/index.js && python3 /app/parser/parser.py' > /start.sh && chmod +x /start.sh

# Default CMD
CMD cron && tail -f /var/log/cron/cron.log

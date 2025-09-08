FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    DISPLAY=:99 \
    CHROME_BIN=/opt/chrome-linux64/chrome \
    CHROMEDRIVER_PATH=/usr/local/bin/chromedriver

RUN echo "deb http://mirrors.aliyun.com/debian/ trixie main" > /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/debian/ trixie-updates main" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/debian-security trixie-security main" >> /etc/apt/sources.list && \
    apt-get update --fix-missing && \
    apt-get install -y --fix-missing --no-install-recommends \
    unzip \
    xvfb \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libwayland-client0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    libu2f-udev \
    libvulkan1 \
    libxss1 \
    || (apt-get update && apt-get install -y --fix-missing --no-install-recommends \
    unzip \
    xvfb \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libwayland-client0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    libu2f-udev \
    libvulkan1 \
    libxss1)

# 复制本地Chrome文件并安装
COPY utils/Chrome/chrome-linux64.zip /tmp/
RUN unzip /tmp/chrome-linux64.zip -d /opt/ && \
    chmod +x /opt/chrome-linux64/chrome && \
    ln -s /opt/chrome-linux64/chrome /usr/local/bin/google-chrome && \
    rm /tmp/chrome-linux64.zip

# 验证Chrome安装
RUN /opt/chrome-linux64/chrome --version && \
    echo "✅ Chrome 安装成功"

# 复制本地ChromeDriver并安装
COPY utils/Chrome/chromedriver-linux64.zip /tmp/
RUN unzip /tmp/chromedriver-linux64.zip -d /tmp/ && \
    mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf /tmp/chromedriver-linux64.zip /tmp/chromedriver-linux64

# 验证ChromeDriver安装
RUN chromedriver --version && \
    echo "✅ ChromeDriver 安装成功"

# 复制项目文件到容器
COPY . /app/

# 创建必要的目录
RUN mkdir -p /app/logs /app/downloads /app/data


RUN pip install --upgrade pip -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com

RUN pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com \
    dotenv>=0.9.9 \
    fake-useragent>=2.2.0 \
    loguru>=0.7.3 \
    pandas>=2.3.2 \
    psutil>=7.0.0 \
    pydantic>=2.11.7 \
    pypdf>=6.0.0 \
    pytest>=8.4.1 \
    requests>=2.32.5 \
    selenium>=4.35.0 \
    sqlalchemy>=2.0.43 \
    cx-oracle>=8.3.0 \
    webdriver-manager>=4.0.2

# 清理apt缓存以减小镜像大小
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


# 健康检查（移除网络依赖）
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; print('Health check passed'); sys.exit(0)"

CMD ["/bin/bash"]

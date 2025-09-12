FROM python:3.11

# 设置工作目录
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    DISPLAY=:99 \
    CHROME_BIN=/opt/chrome-linux64/chrome \
    CHROMEDRIVER_PATH=/usr/local/bin/chromedriver


RUN echo "deb http://mirrors.aliyun.com/debian bookworm main contrib non-free non-free-firmware" > /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/debian bookworm-updates main contrib non-free non-free-firmware" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/debian-security bookworm-security main contrib non-free non-free-firmware" >> /etc/apt/sources.list && \
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

# 安装 Oracle Instant Client 和基础工具
RUN apt-get update && apt-get install -y --no-install-recommends \
    libaio1 \
    curl \
    unzip \
    alien \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/oracle && \
    curl -o /tmp/instantclient-basiclite.zip https://download.oracle.com/otn_software/linux/instantclient/219000/instantclient-basiclite-linux.x64-21.9.0.0.0dbru.zip && \
    unzip /tmp/instantclient-basiclite.zip -d /opt/oracle && \
    rm -f /tmp/instantclient-basiclite.zip && \
    ln -s /opt/oracle/instantclient_* /opt/oracle/instantclient

# 配置 Oracle 环境变量
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient \
    PATH=/opt/oracle/instantclient:$PATH

# 安装 OceanBase 客户端组件
COPY utils/rpm/libobclient-2.2.11-42025062010.el7.x86_64.rpm /tmp/
COPY utils/rpm/obci-2.1.1-362025071011.el7.x86_64.rpm /tmp/

RUN alien -d /tmp/libobclient-2.2.11-42025062010.el7.x86_64.rpm && \
    alien -d /tmp/obci-2.1.1-362025071011.el7.x86_64.rpm && \
    find / -name "libobclient_*.deb" -o -name "obci_*.deb" && \
    dpkg -i ./libobclient_*.deb && \
    dpkg -i ./obci_*.deb && \
    rm -f *.deb /tmp/*.rpm && \
    apt-get install -f -y


# 安装oracle 模块 cd /u01/obclient/python/
RUN cd /u01/obclient/python/ && tar -xvf cx_Oracle-8.3.0.tar.gz && \
    cd cx_Oracle-8.3.0 && python setup.py install

ENV LD_LIBRARY_PATH=/u01/obclient/lib/:/opt/oracle/instantclient:$LD_LIBRARY_PATH \
    PATH=/u01/obclient/bin:$PATH

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

# 先安装基础依赖
RUN pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com \
    setuptools \
    wheel

# 安装Python包 - 使用项目实际依赖
RUN pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com \
    python-dotenv>=0.9.9 \
    fake-useragent>=2.2.0 \
    loguru>=0.7.3 \
    pandas>=2.3.2 \
    psutil>=7.0.0 \
    pydantic>=2.11.7 \
    pytest>=8.4.1 \
    requests>=2.32.5 \
    selenium>=4.35.0 \
    webdriver-manager>=4.0.2 \

# 在安装完成后清理编译工具以减小镜像大小
RUN apt-get remove -y build-essential gcc g++ libc6-dev && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# 健康检查 - 验证关键组件
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import jaydebeapi, selenium, pandas; print('Health check passed'); import sys; sys.exit(0)"

CMD ["/bin/bash"]

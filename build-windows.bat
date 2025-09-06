@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion

REM ========================================
REM   Windows环境Docker镜像构建打包脚本
REM ========================================

set IMAGE_NAME=dwh-builder
set IMAGE_TAG=latest
set EXPORT_FILE=dwh-builder-offline.tar

echo ========================================
echo   Windows环境Docker镜像构建打包脚本
echo ========================================

REM 检查Docker是否可用
docker --version >nul 2>&1
if errorlevel 1 (
    echo ❌ 错误: Docker未安装或不可用
    echo 请确保Docker Desktop已安装并正在运行
    pause
    exit /b 1
)

echo ✅ Docker可用

REM 检查必要文件是否存在
if not exist "utils\Chrome\chrome-linux64.zip" (
    echo ❌ 错误: 缺少Chrome文件 utils\Chrome\chrome-linux64.zip
    pause
    exit /b 1
)

if not exist "utils\Chrome\chromedriver-linux64.zip" (
    echo ❌ 错误: 缺少ChromeDriver文件 utils\Chrome\chromedriver-linux64.zip
    pause
    exit /b 1
)

echo ✅ Chrome相关文件检查通过

REM 显示构建信息
echo.
echo 构建配置:
echo - 镜像名称: %IMAGE_NAME%:%IMAGE_TAG%
echo - 导出文件: %EXPORT_FILE%
echo - 使用本地Chrome文件: 是
echo.

REM 确认是否继续
set /p confirm=是否继续构建? (y/N):
if /i not "%confirm%"=="y" (
    echo 构建已取消
    pause
    exit /b 0
)

REM 构建镜像
echo.
echo 🔨 开始构建Docker镜像...
echo 这可能需要几分钟时间，请耐心等待...

docker build -t %IMAGE_NAME%:%IMAGE_TAG% .

if errorlevel 1 (
    echo ❌ Docker镜像构建失败
    pause
    exit /b 1
)

echo ✅ Docker镜像构建成功

REM 导出镜像
echo.
echo 📦 导出Docker镜像为tar文件...
docker save -o %EXPORT_FILE% %IMAGE_NAME%:%IMAGE_TAG%

if errorlevel 1 (
    echo ❌ Docker镜像导出失败
    pause
    exit /b 1
)

echo ✅ Docker镜像导出成功: %EXPORT_FILE%

REM 显示文件大小
for %%I in (%EXPORT_FILE%) do set FILESIZE=%%~zI
set /a FILESIZE_MB=%FILESIZE%/1024/1024
echo 📊 文件大小: %FILESIZE_MB% MB

echo.
echo ========================================
echo   构建完成
echo ========================================
echo 离线镜像文件: %EXPORT_FILE%
echo.
echo 传输到内网服务器后的使用步骤:
echo 1. 上传 %EXPORT_FILE% 到内网服务器
echo 2. 在服务器上执行: docker load -i %EXPORT_FILE%
echo 3. 运行容器: docker run -d --name dwh-builder %IMAGE_NAME%:%IMAGE_TAG%
echo.
echo 完整运行命令:
echo docker run -d \
echo   --name dwh-builder \
echo   -v $(pwd)/logs:/app/logs \
echo   -v $(pwd)/data:/app/data \
echo   --shm-size=2g \
echo   %IMAGE_NAME%:%IMAGE_TAG%
echo ========================================

pause

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /opt/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY simulator ./simulator
COPY scripts ./scripts

# SQLite 监测数据目录（compose 用命名卷挂载到此处）
RUN mkdir -p /data

# 采集服务
EXPOSE 8000
# 模拟控制器（Modbus 5020 / 控制 API 8081）
EXPOSE 5020 8081

# 默认启动采集服务；模拟器服务在 compose 中覆盖 command。
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

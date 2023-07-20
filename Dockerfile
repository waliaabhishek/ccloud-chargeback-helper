FROM python:3.11

WORKDIR /app

COPY requirements.txt .
COPY src/  ./
COPY deployables/assets/chargeback_handler/config/config_internal.yaml ./config/config.yaml
RUN pip install --no-cache-dir -r ./requirements.txt

ENTRYPOINT [ "python", "main.py" ]
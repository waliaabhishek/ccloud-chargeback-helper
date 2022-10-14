FROM python:3.10

WORKDIR /usr/src/app

COPY requirements.txt .
COPY src/*.py ./
COPY src/ccloud/*.py ./ccloud/
COPY src/ccloud/core_api/*.py ./ccloud/core_api/
COPY src/ccloud/telemetry_api/*.py ./ccloud/telemetry_api/
COPY src/data_processing/*.py ./data_processing/
COPY config/config_internal.yaml ./config/config.yaml
RUN pip install --no-cache-dir -r ./requirements.txt

ENTRYPOINT [ "python", "main.py" ]
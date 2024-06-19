FROM python:3.7-slim

COPY  ./requirements.txt /app/requirements.txt

RUN   apt-get update && apt-get install -y \
        && pip install --upgrade pip \
        && pip install -r /app/requirements.txt\
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

COPY ../kafka_broker/producer.py /app/producer.py

WORKDIR /app

ENTRYPOINT [ "python"]
CMD ["producer.py"]

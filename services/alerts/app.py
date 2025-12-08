import json
import logging
import os
from collections import deque
from threading import Thread
from time import sleep

from dotenv import load_dotenv
from flask import Flask, Response, jsonify
from kafka import KafkaConsumer
from prometheus_client import Counter, generate_latest, REGISTRY

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092",
)
PROCESSED_TOPIC = os.getenv("PROCESSED_TOPIC", "processed-earthquakes")
MAG_THRESHOLD = float(os.getenv("MAG_THRESHOLD", "5.0"))

app = Flask(__name__)

logger = logging.getLogger("alerts")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def log_event(payload: dict):
    logger.info(json.dumps(payload, ensure_ascii=False))

ALERTS_PUBLISHED = Counter(
    "alerts_published_total",
    "Total de alertas generadas por el servicio de alertas"
)

consumer = KafkaConsumer(
    PROCESSED_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

latest_alerts = deque(maxlen=100)

def alerts_loop():
    log_event({"bitácora": "loop_alertas_iniciado", "threshold": MAG_THRESHOLD})
    for msg in consumer:
        ev = msg.value
        mag = ev.get("magnitude")
        if mag is not None and mag >= MAG_THRESHOLD:
            alert = {
                "event_id": ev.get("event_id"),
                "magnitude": mag,
                "place": ev.get("place"),
                "timestamp": ev.get("time"),
                "severity": ev.get("severity_level"),
            }
            latest_alerts.append(alert)
            ALERTS_PUBLISHED.inc()
            log_event({"bitácora": "alerta_generada", "alerta": alert})

@app.get("/health")
def health():
    return jsonify({"status": "ok"})

@app.get("/metrics")
def metrics():
    return generate_latest(REGISTRY), 200, {"Content-Type": "text/plain; version=0.0.4"}

@app.get("/alerts")
def alerts():
    return jsonify(list(latest_alerts))

@app.get("/alerts/stream")
def alerts_stream():
    def event_stream():
        last_len = 0
        while True:
            if len(latest_alerts) > last_len:
                nuevos = list(latest_alerts)[last_len:]
                for a in nuevos:
                    yield f"data: {json.dumps(a)}\n\n"
                last_len = len(latest_alerts)
            sleep(1)

    return Response(event_stream(), mimetype="text/event-stream")

def start_background_alerts():
    t = Thread(target=alerts_loop, daemon=True)
    t.start()

if __name__ == "__main__":
    start_background_alerts()
    app.run(host="0.0.0.0", port=5000)

import json
import logging
import os
import time
from threading import Thread

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify
from flask_swagger_ui import get_swaggerui_blueprint
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, generate_latest, REGISTRY

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092",
)
RAW_TOPIC = os.getenv("RAW_TOPIC", "raw-earthquakes")
USGS_URL = os.getenv(
    "USGS_URL",
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
)
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

app = Flask(__name__)

logger = logging.getLogger("ingestion")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def log_event(payload: dict):
    logger.info(json.dumps(payload, ensure_ascii=False))

EVENTS_PUBLISHED = Counter(
    "ingestion_events_published_total",
    "Total de eventos publicados a Kafka desde ingestion"
)

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )

producer = None

def send_with_retry(topic: str, value: dict, max_retries: int = 5, base_sleep: float = 1.0):
    for attempt in range(1, max_retries + 1):
        try:
            future = producer.send(topic, value)
            future.get(timeout=10)
            return
        except KafkaError as e:
            log_event({
                "bitácora": "error_envio_kafka",
                "topic": topic,
                "intento": attempt,
                "error": str(e),
            })
            time.sleep(base_sleep * attempt)
    log_event({
        "bitácora": "fallo_envio_definitivo",
        "topic": topic,
        "max_reintentos": max_retries,
    })

def normalize_event(feature: dict) -> dict:
    props = feature.get("properties", {}) or {}
    geom = feature.get("geometry", {}) or {}
    coords = geom.get("coordinates", [None, None, None])

    return {
        "event_id": feature.get("id"),
        "magnitude": props.get("mag"),
        "place": props.get("place"),
        "time": props.get("time"),
        "depth": coords[2],
        "longitude": coords[0],
        "latitude": coords[1],
        "raw": feature,
    }

def poll_loop():
    global producer
    while True:
        try:
            resp = requests.get(USGS_URL, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            features = data.get("features", []) or []
            log_event({"bitácora": "usgs_respuesta", "total_features": len(features)})

            for feature in features:
                ev = normalize_event(feature)
                send_with_retry(RAW_TOPIC, ev)
                EVENTS_PUBLISHED.inc()
                log_event({
                    "bitácora": "evento_publicado",
                    "topic": RAW_TOPIC,
                    "event_id": ev.get("event_id"),
                })

        except Exception as e:
            log_event({"bitácora": "error_usgs", "error": str(e)})

        time.sleep(POLL_INTERVAL)

def start_background_poll():
    t = Thread(target=poll_loop, daemon=True)
    t.start()

@app.get("/health")
def health():
    return jsonify({"status": "ok"})

@app.get("/metrics")
def metrics():
    return generate_latest(REGISTRY), 200, {"Content-Type": "text/plain; version=0.0.4"}

SWAGGER_URL = "/swagger"
API_URL = "/swagger.json"
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={"app_name": "Earthquake Ingestion Service"},
)
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

@app.get("/swagger.json")
def swagger_spec():
    return jsonify({
        "openapi": "3.0.0",
        "info": {"title": "Ingestion API", "version": "1.0.0"},
        "paths": {
            "/health": {"get": {"summary": "Health check"}},
            "/metrics": {"get": {"summary": "Prometheus metrics"}},
        },
    })

if __name__ == "__main__":
    producer = create_producer()
    start_background_poll()
    app.run(host="0.0.0.0", port=5000)

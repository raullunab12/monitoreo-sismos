import json
import logging
import os
from threading import Thread

import psycopg2
from dotenv import load_dotenv
from flask import Flask, jsonify
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, generate_latest, REGISTRY

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092",
)
RAW_TOPIC = os.getenv("RAW_TOPIC", "raw-earthquakes")
PROCESSED_TOPIC = os.getenv("PROCESSED_TOPIC", "processed-earthquakes")

DB_HOST = os.getenv("DB_HOST", "postgres.database.svc.cluster.local")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "earthquakes")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "example")

app = Flask(__name__)

logger = logging.getLogger("processing")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def log_event(payload: dict):
    logger.info(json.dumps(payload, ensure_ascii=False))

EVENTS_PROCESSED = Counter(
    "processing_events_total",
    "Total de eventos procesados por el servicio de procesamiento"
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

def severity_from_magnitude(mag):
    if mag is None:
        return "unknown"
    if mag >= 7.0:
        return "critical"
    if mag >= 5.0:
        return "high"
    if mag >= 3.0:
        return "moderate"
    return "low"

def processing_loop():
    conn = get_db_conn()
    cur = conn.cursor()
    log_event({"bitácora": "loop_procesamiento_iniciado"})
    for msg in consumer:
        ev = msg.value
        ev["severity_level"] = severity_from_magnitude(ev.get("magnitude"))

        try:
            sql = (
                "INSERT INTO earthquakes ("
                "event_id, magnitude, depth, latitude, longitude, "
                "timestamp, place, severity_level, raw"
                ") VALUES ("
                "%s,%s,%s,%s,%s,to_timestamp(%s/1000.0),%s,%s,%s"
                ")"
            )
            cur.execute(
                sql,
                (
                    ev.get("event_id"),
                    ev.get("magnitude"),
                    ev.get("depth"),
                    ev.get("latitude"),
                    ev.get("longitude"),
                    ev.get("time"),
                    ev.get("place"),
                    ev.get("severity_level"),
                    json.dumps(ev.get("raw")),
                ),
            )
            conn.commit()
            producer.send(PROCESSED_TOPIC, ev)
            EVENTS_PROCESSED.inc()
            log_event({
                "bitácora": "evento_procesado",
                "event_id": ev.get("event_id"),
                "severity_level": ev.get("severity_level"),
            })
        except Exception as e:
            conn.rollback()
            log_event({"bitácora": "error_db_o_procesamiento", "error": str(e)})

@app.get("/health")
def health():
    return jsonify({"status": "ok"})

@app.get("/metrics")
def metrics():
    return generate_latest(REGISTRY), 200, {"Content-Type": "text/plain; version=0.0.4"}

def start_background_processing():
    t = Thread(target=processing_loop, daemon=True)
    t.start()

if __name__ == "__main__":
    start_background_processing()
    app.run(host="0.0.0.0", port=5000)

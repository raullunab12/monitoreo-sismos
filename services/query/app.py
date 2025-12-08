import os

import psycopg2
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from prometheus_client import generate_latest, REGISTRY
from psycopg2.extras import RealDictCursor

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "postgres.database.svc.cluster.local")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "earthquakes")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "example")

app = Flask(__name__)

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

@app.get("/health")
def health():
    return jsonify({"status": "ok"})

@app.get("/metrics")
def metrics():
    return generate_latest(REGISTRY), 200, {"Content-Type": "text/plain; version=0.0.4"}

@app.get("/earthquakes/recent")
def recent():
    limit = int(request.args.get("limit", "20"))
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT * FROM earthquakes ORDER BY timestamp DESC LIMIT %s",
        (limit,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)

@app.get("/earthquakes/by-magnitude")
def by_magnitude():
    minm = float(request.args.get("min", "0"))
    maxm = float(request.args.get("max", "10"))
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT * FROM earthquakes
        WHERE magnitude BETWEEN %s AND %s
        ORDER BY timestamp DESC
        LIMIT 100
        """,
        (minm, maxm),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)

@app.get("/earthquakes/by-region")
def by_region():
    q = request.args.get("q", "")
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT * FROM earthquakes
        WHERE place ILIKE %s
        ORDER BY timestamp DESC
        LIMIT 100
        """,
        (f"%{q}%",),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

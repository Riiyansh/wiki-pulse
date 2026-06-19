"""
Wikipedia EventStream → Kafka Producer

Connects to Wikipedia's real-time SSE stream (no API key needed).
Publishes every edit/new-page event to the Kafka topic "wiki-edits".

Stream docs: https://stream.wikimedia.org/v2/stream/recentchange
"""

import json
import os
import time
import logging
import sseclient
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = "wiki-edits"
STREAM_URL      = "https://stream.wikimedia.org/v2/stream/recentchange"

# Only keep article edits (namespace 0) and new pages across all wikis
ALLOWED_NS = {0}


def make_producer(retries: int = 20) -> KafkaProducer:
    for i in range(retries):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                compression_type="gzip",
                linger_ms=50,
                batch_size=16384,
            )
            log.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return p
        except NoBrokersAvailable:
            log.warning(f"Kafka not ready, retry {i+1}/{retries}…")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after retries")


def parse_event(data: dict) -> dict | None:
    """Extract relevant fields from a Wikipedia recentchange event."""
    if data.get("type") not in ("edit", "new"):
        return None
    if data.get("namespace") not in ALLOWED_NS:
        return None

    meta = data.get("meta", {})
    lang = data.get("wiki", "").replace("wiki", "") or "en"

    return {
        "event_time":   meta.get("dt") or data.get("timestamp", ""),
        "title":        data.get("title", ""),
        "wiki":         data.get("wiki", ""),
        "language":     lang,
        "user_name":    data.get("user", ""),
        "is_bot":       data.get("bot", False),
        "is_new_page":  data.get("type") == "new",
        "delta_bytes":  (data.get("length", {}).get("new", 0) or 0) -
                        (data.get("length", {}).get("old", 0) or 0),
        "comment":      (data.get("comment", "") or "")[:200],
        "server_url":   data.get("server_url", ""),
        "namespace":    data.get("namespace", 0),
    }


def stream(producer: KafkaProducer):
    log.info(f"Connecting to Wikipedia EventStream…")
    resp = requests.get(STREAM_URL, stream=True, timeout=30,
                        headers={"User-Agent": "WikiPulse/1.0 (educational project)"})
    client = sseclient.SSEClient(resp)

    count = 0
    for event in client.events():
        if not event.data or event.data == "":
            continue
        try:
            raw = json.loads(event.data)
            parsed = parse_event(raw)
            if parsed:
                producer.send(TOPIC, parsed)
                count += 1
                if count % 500 == 0:
                    log.info(f"Published {count} events")
        except Exception as e:
            log.debug(f"Parse error: {e}")


def main():
    producer = make_producer()
    while True:
        try:
            stream(producer)
        except Exception as e:
            log.error(f"Stream error: {e}, reconnecting in 5s…")
            time.sleep(5)


if __name__ == "__main__":
    main()

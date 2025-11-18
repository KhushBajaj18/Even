# storage-layer/utils/db_writer.py

import psycopg2
from psycopg2.extras import Json
import os

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "even_db"
DB_USER = "even_user"
DB_PASS = "even_pass"

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME
    )

def write_event(event):
    try:
        conn = get_conn()
        cur = conn.cursor()

        sql = """
        INSERT INTO feedback_events (
            event_id, customer_id, source, source_id, ts,
            raw_text, redacted_text, raw_blob_url,
            metadata, canonical_json,
            language, has_pii, quality_score
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (event_id) DO NOTHING;
        """

        cur.execute(sql, (
            event.get("event_id"),
            event.get("customer_id"),
            event.get("source"),
            event.get("source_id"),
            event.get("timestamp"),
            event.get("raw_text"),
            event.get("redacted_text"),
            event.get("raw_blob_url"),
            Json(event.get("metadata")),
            Json(event),
            event.get("language"),
            event.get("has_pii"),
            event.get("quality_score")
        ))

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print("DB error:", e)

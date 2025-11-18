# adapters/adapter_csat.py
from utils.cleaning import clean_text, redact_pii
import uuid, datetime

def canonical_common(source, source_id, customer_id, ts, raw_text=None, raw_blob_url=None, metadata=None):
    now = datetime.datetime.utcnow().isoformat() + "Z"
    event = {
        "event_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "source": source,
        "source_id": source_id,
        "timestamp": ts,
        "raw_text": raw_text,
        "raw_blob_url": raw_blob_url,
        "ingested_at": now,
        "language": None,
        "redacted_text": None,
        "processed_at": None,
        "metadata": metadata or {}
    }
    event["raw_text"] = clean_text(event["raw_text"])
    event["redacted_text"] = redact_pii(event["raw_text"]) if event["raw_text"] else None
    return event

def convert_csat(raw):
    source_id = raw.get("survey_id") or raw.get("id")
    cust_id = raw.get("customer_id")
    text = raw.get("comment") or ""
    meta = {"score": raw.get("score")}
    return canonical_common("csat", source_id, cust_id, raw.get("timestamp"), raw_text=text, metadata=meta)

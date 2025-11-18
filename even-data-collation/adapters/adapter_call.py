# adapters/adapter_call.py
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

def convert_call(raw):
    source_id = raw.get("call_id") or raw.get("id")
    cust_id = raw.get("customer_id")
    transcript = raw.get("transcript")
    meta = {"call_duration_sec": raw.get("call_duration_sec"), "audio_url": raw.get("audio_url")}
    return canonical_common("call", source_id, cust_id, raw.get("timestamp"), raw_text=transcript, raw_blob_url=raw.get("audio_url"), metadata=meta)

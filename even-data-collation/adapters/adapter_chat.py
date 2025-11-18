# adapters/adapter_chat.py
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

def convert_chat(raw):
    source_id = raw.get("conversation_id") or raw.get("id")
    customer = raw.get("customer") or {}
    cust_id = customer.get("customer_id") or customer.get("id")
    text = raw.get("message") or raw.get("text")
    meta = {"agent_id": raw.get("agent_id")}
    return canonical_common("chat", source_id, cust_id, raw.get("timestamp"), raw_text=text, metadata=meta)

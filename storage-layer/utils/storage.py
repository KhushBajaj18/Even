# storage-layer/utils/storage.py

import json
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent / "storage"

RAW_DIR = BASE / "raw"
CANONICAL_DIR = BASE / "canonical" / "events"

def save_raw(source, source_id, raw_payload):
    folder = RAW_DIR / source
    folder.mkdir(parents=True, exist_ok=True)
    file = folder / f"{source_id}.json"
    file.write_text(json.dumps(raw_payload, indent=2, ensure_ascii=False))
    return str(file)

def save_canonical(event):
    CANONICAL_DIR.mkdir(parents=True, exist_ok=True)
    file = CANONICAL_DIR / f"{event['event_id']}.json"
    file.write_text(json.dumps(event, indent=2, ensure_ascii=False))
    return str(file)

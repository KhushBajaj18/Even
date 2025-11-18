# ingest_to_canonical.py

import sys
from pathlib import Path
import pandas as pd

# -------------------------
# ADD STORAGE LAYER IMPORTS
# -------------------------

# BASE_DIR = Even/
BASE_DIR = Path(__file__).resolve().parent.parent

# Path to: Even/storage-layer/utils/
STORAGE_UTILS = BASE_DIR / "storage-layer" / "utils"
sys.path.append(str(STORAGE_UTILS))

from db_writer import write_event
from storage import save_raw, save_canonical

# -------------------------
# IMPORT EXISTING UTILITIES
# -------------------------

from utils.loader import load_json_files
from adapters.adapter_chat import convert_chat
from adapters.adapter_email import convert_email
from adapters.adapter_call import convert_call
from adapters.adapter_csat import convert_csat
from adapters.adapter_grievance import convert_grievance

BASE = Path('.')
OUTPUT_DIR = BASE / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# -------------------------
# PROCESS FOLDER FUNCTION
# -------------------------
def process_folder(folder_path, source_name, convert_fn):
    rows = []
    entries = load_json_files(folder_path)

    for fname, raw in entries:

        # 1. GET SOURCE ID (fallbacks)
        source_id = (
            raw.get("id")
            or raw.get("conversation_id")
            or raw.get("call_id")
            or raw.get("email_id")
            or raw.get("csat_id")
            or raw.get("grievance_id")
            or Path(fname).stem
        )

        # 2. Save RAW FILE to storage-layer
        save_raw(source_name, source_id, raw)

        # 3. Convert to canonical event
        ev = convert_fn(raw)

        # 4. Save canonical JSON
        save_canonical(ev)

        # 5. Insert into Postgres DB
        write_event(ev)

        # 6. Append for CSV output
        rows.append(ev)

    return rows

# -------------------------
# MAIN PIPELINE
# -------------------------
def main():
    base_src = BASE / "fake_sources"

    chat = process_folder(base_src / "chat", "chat", convert_chat)
    email = process_folder(base_src / "email", "email", convert_email)
    call = process_folder(base_src / "call", "call", convert_call)
    csat = process_folder(base_src / "csat", "csat", convert_csat)
    grievance = process_folder(base_src / "grievance", "grievance", convert_grievance)

    all_events = chat + email + call + csat + grievance

    if not all_events:
        print("No events found")
        return

    df = pd.DataFrame(all_events)
    out = OUTPUT_DIR / "feedback_events.csv"
    df.to_csv(out, index=False)
    print(f"WROTE {len(df)} events → {out}")

if __name__ == "__main__":
    main()

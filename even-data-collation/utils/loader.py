# utils/loader.py
from pathlib import Path
import json

def load_json_files(folder_path):
    folder = Path(folder_path)
    results = []
    for f in folder.glob("*.json"):
        data = json.loads(f.read_text(encoding='utf-8'))
        results.append((f.name, data))
    return results

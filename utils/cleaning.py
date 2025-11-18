# utils/cleaning.py
import re
from html import unescape

PHONE_RE = re.compile(r'\b(?:\+91[-\s]?|0)?[6-9]\d{9}\b')
EMAIL_RE = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b')
ID_RE = re.compile(r'\b(?:CLM|CLAIM|ID|ID:)[-_]?\d{3,10}\b', re.IGNORECASE)

def strip_html(text):
    if not text:
        return text
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    return ' '.join(text.split())

def redact_pii(text):
    if not text:
        return text
    t = text
    t = PHONE_RE.sub('[REDACTED_PHONE]', t)
    t = EMAIL_RE.sub('[REDACTED_EMAIL]', t)
    t = ID_RE.sub('[REDACTED_ID]', t)
    t = re.sub(r'\b\d{6,}\b', '[REDACTED_NUMBER]', t)
    return t

def clean_text(text):
    if text is None:
        return None
    text = strip_html(text)
    text = text.strip()
    text = re.sub(r'\s+', ' ', text)
    return text if text else None

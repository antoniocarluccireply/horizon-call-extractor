import os
import re
import uuid
import json
import calendar
import boto3
import urllib.request
import urllib.error
import traceback
from datetime import date
from pypdf import PdfReader
from openpyxl import Workbook

from parser_horizon import parse_calls

s3 = boto3.client(
    "s3",
    region_name="eu-central-1",
    endpoint_url="https://s3.eu-central-1.amazonaws.com",
)

BUCKET = os.environ.get("BUCKET_NAME", "")
def _require_bucket():
    if not BUCKET:
        # Non blocchiamo la UI, ma blocchiamo le API che richiedono S3
        raise RuntimeError("Missing env var BUCKET_NAME")

# --- OpenAI (optional) ---
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")  # keep secret in Lambda env vars
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5-mini")
OPENAI_MAX_TOPICS = int(os.environ.get("OPENAI_MAX_TOPICS", "25"))
OPENAI_BODY_MAX_CHARS = int(os.environ.get("OPENAI_BODY_MAX_CHARS", "6000"))
DEFAULT_MIN_BUDGET_M = float(os.environ.get("DEFAULT_MIN_BUDGET_M", "0"))


def _safe_base_name(file_name: str) -> str:
    base = os.path.basename(file_name or "").strip()
    if not base:
        return "file"

    base = re.sub(r"[\\/:*?\"<>|]", "_", base)
    base = base.strip(". ")
    name, _ext = os.path.splitext(base)
    cleaned = name or "file"
    return cleaned[:120]


def _topic_url(topic_id: str) -> str:
    tid = (topic_id or "").strip()
    return (
        f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-details/{tid}"
        if tid
        else ""
    )


def extract_text(pdf_path: str) -> str:
    """
    Extract text with explicit page markers so parser_horizon can set 'page'.
    """
    reader = PdfReader(pdf_path)
    chunks = []
    for idx, p in enumerate(reader.pages, start=1):
        chunks.append(f"\n<<<PAGE {idx}>>>\n")
        chunks.append(p.extract_text() or "")
    return "\n".join(chunks)


def _compute_budget_per_project_m(row):
    vals = []
    for key in (
        "budget_per_project_min_eur_m",
        "budget_per_project_max_eur_m",
        "budget_per_project_m",
    ):
        v = row.get(key)
        if isinstance(v, (int, float)):
            vals.append(float(v))
    if vals:
        return min(vals)
    return None


def _matches_prefix(value: str, prefix: str) -> bool:
    pref = (prefix or "").strip()
    if not pref:
        return True
    if value is None:
        return False
    return str(value).strip().lower().startswith(pref.lower())


def write_xlsx(rows, xlsx_path: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "calls"

    headers = [
        "cluster",
        "stage",
        "call_round",
        "page",
        "call_id",
        "topic_id",
        "topic_title",
        "topic_description",
        "action_type",
        "trl",
        "budget_eur_m",
        "budget_per_project_min_eur_m",
        "budget_per_project_max_eur_m",
        "projects",
        "opening_date",
        "deadline_date",
    ]
    ws.append(headers)

    for r in rows:
        row_values = [r.get(h) for h in headers]
        ws.append(row_values)

    # apply hyperlink to topic_id column (6th col)
    for idx, r in enumerate(rows, start=2):  # header is row 1
        cell = ws.cell(row=idx, column=6)
        url = _topic_url(r.get("topic_id"))
        if url:
            cell.hyperlink = url
            cell.style = "Hyperlink"

    wb.save(xlsx_path)


def _parse_date(s: str):
    """
    Parse YYYY, YYYY-MM, or YYYY-MM-DD into a date object.
    Also supports textual dates like "23 Sep 2026" or "23 September 2026".
    Returns None when parsing fails.
    """
    txt = (s or "").strip()
    if not txt:
        return None

    # Remove trailing punctuation that often appears in PDF extracts
    txt = txt.rstrip(".,;")

    m_full = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", txt)
    if m_full:
        y, mo, d = int(m_full.group(1)), int(m_full.group(2)), int(m_full.group(3))
        try:
            return date(y, mo, d)
        except ValueError:
            return None

    m_month = re.match(r"^(\d{4})-(\d{2})$", txt)
    if m_month:
        y, mo = int(m_month.group(1)), int(m_month.group(2))
        try:
            return date(y, mo, calendar.monthrange(y, mo)[1])
        except ValueError:
            return None

    m_year = re.match(r"^(\d{4})$", txt)
    if m_year:
        y = int(m_year.group(1))
        try:
            return date(y, 12, 31)
        except ValueError:
            return None

    m_day_name = re.match(r"^(\d{1,2})\s+([A-Za-z]{3,})\.?,?\s+(\d{4})$", txt)
    if m_day_name:
        day = int(m_day_name.group(1))
        mon_raw = m_day_name.group(2).strip().lower().rstrip(".")
        year = int(m_day_name.group(3))
        months = {
            "jan": 1, "january": 1,
            "feb": 2, "february": 2,
            "mar": 3, "march": 3,
            "apr": 4, "april": 4,
            "may": 5,
            "jun": 6, "june": 6,
            "jul": 7, "july": 7,
            "aug": 8, "august": 8,
            "sep": 9, "sept": 9, "september": 9,
            "oct": 10, "october": 10,
            "nov": 11, "november": 11,
            "dec": 12, "december": 12,
            # Italian month names (PDFs sometimes localized)
            "gen": 1, "gennaio": 1,
            "febbraio": 2,
            "marzo": 3,
            "aprile": 4,
            "maggio": 5,
            "giugno": 6,
            "luglio": 7,
            "agosto": 8,
            "settembre": 9,
            "ottobre": 10,
            "novembre": 11,
            "dicembre": 12,
        }
        mo = months.get(mon_raw)
        if mo:
            try:
                return date(year, mo, day)
            except ValueError:
                return None

    m_day_slash = re.match(r"^(\d{1,2})[./](\d{1,2})[./](\d{4})$", txt)
    if m_day_slash:
        d, mo, y = int(m_day_slash.group(1)), int(m_day_slash.group(2)), int(m_day_slash.group(3))
        try:
            return date(y, mo, d)
        except ValueError:
            return None

    return None


def _parse_filter_range(filter_value: str):
    """
    Convert user filter into an (start, end) tuple.
    - YYYY -> (None, 31 Dec YYYY)
    - YYYY-Qx -> (None, end_of_quarter)
    - YYYY-MM -> (None, end_of_month)
    - YYYY-MM-DD -> (None, that day)
    Returns None if the filter is empty or invalid.
    """
    txt = (filter_value or "").strip()
    if not txt:
        return None

    m_year = re.match(r"^(\d{4})$", txt)
    if m_year:
        y = int(m_year.group(1))
        try:
            return (None, date(y, 12, 31))
        except ValueError:
            return None

    m_quarter = re.match(r"^(\d{4})-Q([1-4])$", txt, flags=re.IGNORECASE)
    if m_quarter:
        y = int(m_quarter.group(1))
        q = int(m_quarter.group(2))
        month_end = q * 3
        try:
            end_day = calendar.monthrange(y, month_end)[1]
            return (None, date(y, month_end, end_day))
        except ValueError:
            return None

    m_month = re.match(r"^(\d{4})-(\d{2})$", txt)
    if m_month:
        y, mo = int(m_month.group(1)), int(m_month.group(2))
        try:
            end_day = calendar.monthrange(y, mo)[1]
            return (None, date(y, mo, end_day))
        except ValueError:
            return None

    m_day = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", txt)
    if m_day:
        y, mo, d = int(m_day.group(1)), int(m_day.group(2)), int(m_day.group(3))
        try:
            return (None, date(y, mo, d))
        except ValueError:
            return None

    return None


def _date_filter_match(value: str, filter_value: str) -> bool:
    """
    Match dates using inclusive upper-bound logic:
    - If filter is a valid date/period, include rows with dates <= end_of_period.
    - Otherwise, fallback to prefix match to avoid breaking existing inputs.
    """
    rng = _parse_filter_range(filter_value)
    if not rng:
        return _matches_prefix(value, filter_value)

    _start, end = rng
    row_date = _parse_date(value)
    if not row_date:
        return False

    if end and row_date > end:
        return False
    return True


def filter_rows(
    rows,
    action_types=None,
    min_budget_m: float = None,
    opening_filter: str = "",
    deadline_filter: str = "",
):
    allowed = None
    if action_types:
        allowed = {str(t).upper() for t in action_types if str(t).strip()}
        if not allowed:
            allowed = None

    filtered = []
    for r in rows:
        if allowed is not None:
            cur = (r.get("action_type") or "").upper()
            if cur not in allowed:
                continue

        if min_budget_m is not None:
            budget_val = r.get("budget_per_project_min_eur_m")
            if not isinstance(budget_val, (int, float)):
                budget_val = _compute_budget_per_project_m(r)
            if budget_val is None:
                budget_val = 0.0
            if budget_val < min_budget_m:
                continue

        if not _date_filter_match(r.get("opening_date"), opening_filter):
            continue

        if not _date_filter_match(r.get("deadline_date"), deadline_filter):
            continue

        filtered.append(r)

    return filtered


# --- OpenAI helpers (Responses API) ---
def _extract_output_text(resp_json: dict) -> str:
    # Some responses include output_text directly
    txt = (resp_json.get("output_text") or "").strip()
    if txt:
        return txt

    # Otherwise parse output array
    out = resp_json.get("output") or []
    parts = []
    for item in out:
        content = item.get("content") or []
        for c in content:
            # depending on the exact shape, you may see output_text or text
            if c.get("type") == "output_text" and c.get("text"):
                parts.append(c["text"])
            elif c.get("type") == "text" and c.get("text"):
                parts.append(c["text"])
    return "\n".join(p.strip() for p in parts if p and p.strip()).strip()


def _openai_topic_description(topic_id: str, topic_title: str, body_text: str, cache: dict) -> str:
    """
    Return a short English summary (max 2 sentences) using ONLY the provided text.
    """
    if not OPENAI_API_KEY:
        return ""

    clean_body = (body_text or "").strip()
    if not clean_body:
        return ""

    if len(clean_body) > OPENAI_BODY_MAX_CHARS:
        clean_body = clean_body[:OPENAI_BODY_MAX_CHARS]

    if clean_body in cache:
        return cache[clean_body]

    instructions = (
        "Summarize only what is present in the provided text.\n"
        "Do not add requirements, dates, numbers, or claims not present.\n"
        "Return 2 short sentences maximum in English using simple language (under 240 characters)."
    )

    user_input = (
        f"Topic ID: {topic_id}\n"
        f"Title: {topic_title}\n\n"
        "Text from PDF:\n"
        f"{clean_body}"
    )

    payload = {
        "model": OPENAI_MODEL,
        "instructions": instructions,
        "input": user_input,
        "store": False,
    }

    req = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode("utf-8"))
            summary = _extract_output_text(data).strip()
            if summary:
                sentences = re.split(r"(?<=[.!?])\s+", summary)
                summary = " ".join([p for p in sentences if p][:2]).strip()
            if len(summary) > 240:
                summary = summary[:240].rstrip()
            cache[clean_body] = summary
            return summary
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        cache[clean_body] = ""
        return ""
    except Exception:
        cache[clean_body] = ""
        return ""


# --- HTML UI ---
HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Horizon Call Extractor</title>
  <style>
    :root{
      --bg:#f9f2ec;
      --card:rgba(255,255,255,0.94);
      --stroke:rgba(255,255,255,0.55);
      --text:#1f0f0f;
      --muted:#5d2e2e;
      --muted2:#7a4a4a;

      --accent:#d9a441;   /* gold */
      --accent-strong:#b97a24;
      --good:#2f855a;
      --warn:#d97706;
      --bad:#b23b3b;

      --shadow: 0 16px 40px rgba(0,0,0,.22);
      --radius: 16px;
    }

    *{ box-sizing:border-box; }
    html,body{ height:100%; }
    body{
      margin:0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji";
      color: var(--text);
      background: radial-gradient(900px 600px at 12% 8%, rgba(129, 196, 149, 0.2), transparent 55%),
        radial-gradient(1100px 720px at 85% 12%, rgba(255, 255, 255, 0.12), transparent 60%),
        radial-gradient(1200px 900px at 50% -5%, rgba(47, 133, 90, 0.32), transparent 58%),
        linear-gradient(180deg, #2f855a, #276749 40%, #1f4f35);
      position: relative;
      overflow-x:hidden;
    }
    body::before{
      content:"";
      position: fixed;
      inset: 0;
      background-image:
        radial-gradient(18px 18px at 10% 14%, rgba(255,255,255,.35), transparent 62%),
        radial-gradient(22px 22px at 88% 8%, rgba(189, 224, 200, .28), transparent 60%),
        radial-gradient(16px 16px at 16% 85%, rgba(74, 159, 109, .28), transparent 70%),
        radial-gradient(14px 14px at 82% 80%, rgba(200, 230, 210,.30), transparent 68%);
      pointer-events: none;
      mix-blend-mode: screen;
      opacity: .8;
      filter: drop-shadow(0 10px 18px rgba(0,0,0,.15));
    }
    body::after{
      content:"";
      position: fixed;
      inset: 0;
      background-image:
        radial-gradient(1.5px 1.5px at 12% 18%, rgba(255,255,255,.7), transparent 60%),
        radial-gradient(1.5px 1.5px at 32% 32%, rgba(255,255,255,.6), transparent 60%),
        radial-gradient(1.7px 1.7px at 68% 22%, rgba(255,255,255,.8), transparent 60%),
        radial-gradient(1.5px 1.5px at 82% 46%, rgba(255,255,255,.65), transparent 60%),
        radial-gradient(1.5px 1.5px at 55% 78%, rgba(255,255,255,.55), transparent 60%),
        radial-gradient(1.5px 1.5px at 25% 62%, rgba(255,255,255,.5), transparent 60%),
        radial-gradient(1.5px 1.5px at 75% 12%, rgba(255,255,255,.65), transparent 60%),
        radial-gradient(1.5px 1.5px at 46% 12%, rgba(255,255,255,.6), transparent 60%);
      background-size: 260px 260px;
      pointer-events: none;
      opacity: .6;
    }

    .wrap{
      max-width: 1040px;
      margin: 32px auto 60px;
      padding: 0 20px;
    }

    .topbar{
      display:flex;
      align-items:center;
      justify-content:flex-start;
      gap:16px;
      margin-bottom: 18px;
      padding: 12px 12px;
      background: rgba(255,255,255,0.92);
      border-radius: calc(var(--radius) + 4px);
      border: 1px solid var(--stroke);
      box-shadow: 0 12px 30px rgba(0,0,0,.18);
      backdrop-filter: blur(8px);
    }

    .brand{
      display:flex;
      align-items:center;
      gap:14px;
      min-width: 240px;
    }

    .logo{
      width: 112px;
      height: auto;
      display:block;
      border-radius: 12px;
      background: #ffffff;
      padding: 10px 12px;
      box-shadow: 0 10px 26px rgba(0,0,0,.18);
      border: 1px solid rgba(255,255,255,.8);
    }

    .brand h1{
      margin:0;
      font-size: 18px;
      letter-spacing:.2px;
    }
    .brand p{
      margin:4px 0 0;
      color: var(--muted);
      font-size: 13px;
      line-height:1.45;
    }

    @media (max-width: 860px){
      .brand{ min-width: 0; }
      .logo{ width: 96px; }
    }

    .card{
      background: var(--card);
      border: 1px solid var(--stroke);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 20px;
    }

    .card h2{
      margin: 0 0 10px;
      font-size: 16px;
      letter-spacing:.2px;
    }
    .sub{
      margin:0 0 14px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
    }

    .filters{
      margin: 16px 0 18px;
      display:grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 12px;
      align-items:flex-start;
    }
    .textinput{
      width: 100%;
      border: 1px solid #d9d1c4;
      border-radius: var(--radius);
      padding: 10px 12px;
      font-size: 14px;
      background: #fffdfa;
      color: var(--text);
      box-sizing: border-box;
    }
    .textinput:disabled{
      background: #f4f0e8;
      color: var(--muted);
      cursor: not-allowed;
    }
    .filterhint{
      margin-top: 6px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.4;
    }
    .filterlabel{
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      letter-spacing:.3px;
      text-transform: uppercase;
      margin-bottom: 8px;
    }
    .multiselect{
      position: relative;
    }
    .selectbtn{
      width: 100%;
      justify-content: space-between;
      background: linear-gradient(180deg, #f8f5ef, #f3eee6);
      border-color: #d9d1c4;
    }
    .selectbtn .chev{ transition: transform .15s ease; }
    .dropdown{
      position:absolute;
      inset: auto 0 0 0;
      transform: translateY(calc(100% + 6px));
      background: #ffffff;
      border:1px solid #d9d1c4;
      border-radius: 14px;
      box-shadow: 0 14px 28px rgba(28,25,20,.12);
      padding: 10px;
      display:none;
      z-index:5;
    }
    .dropdown.open{ display:block; }
    .menuhead{
      display:flex;
      align-items:center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 6px;
      font-size: 12px;
      color: var(--muted);
    }
    .menuactions{
      display:flex;
      gap: 6px;
    }
    .chipbtn{
      border:1px solid #d9d1c4;
      background:#f5f0e8;
      border-radius: 10px;
      padding: 5px 8px;
      font-size: 11px;
      cursor:pointer;
      color: var(--muted);
    }
    .checklist{
      max-height: 220px;
      overflow:auto;
      display:grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 6px;
    }
    .checkrow{
      display:flex;
      align-items:center;
      gap: 8px;
      padding: 8px 9px;
      border:1px solid #e6ded3;
      border-radius: 12px;
      background:#f9f6f1;
      font-size: 13px;
      color: var(--text);
      transition:.1s ease;
      cursor:pointer;
      user-select:none;
    }
    .checkrow:hover{ border-color: var(--accent); }
    .checkrow input{ accent-color: var(--accent); }

    .sliderwrap{
      background:#f9f6f1;
      border:1px solid #e6ded3;
      border-radius: 14px;
      padding: 12px;
    }
    .bubble{
      display:inline-block;
      padding: 6px 10px;
      border-radius: 10px;
      background: #2f855a;
      color:#fff;
      font-weight: 700;
      font-size: 13px;
      margin-bottom: 8px;
      box-shadow: 0 10px 22px rgba(47,133,90,.24);
    }
    input[type=range]{
      width: 100%;
      accent-color: var(--accent);
    }
    .sliderlabels{
      display:flex;
      align-items:center;
      justify-content: space-between;
      color: var(--muted2);
      font-size: 11px;
      margin-top: 4px;
    }

    .dropzone{
      position: relative;
      border-radius: 16px;
      border: 1px dashed rgba(107,139,95,.45);
      background: #f9f6f1;
      padding: 18px;
      transition: .15s ease;
    }
    .dropzone.drag{
      border-color: var(--accent);
      box-shadow: 0 0 0 4px rgba(107,139,95,.12);
      transform: translateY(-1px);
      background: #f4f0e8;
    }

    .dzrow{
      display:flex;
      align-items:center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }

    .filemeta{
      display:flex;
      flex-direction:column;
      gap: 6px;
      min-width: 220px;
      flex:1 1 auto;
    }

    .filename{
      font-weight: 650;
      font-size: 14px;
      color: var(--text);
      overflow-wrap: anywhere;
      word-break: break-word;
      white-space: normal;
    }
    .filesub{
      color: var(--muted2);
      font-size: 12px;
    }

    .actions{
      display:flex;
      align-items:center;
      gap: 10px;
      flex-wrap: wrap;
    }

    .btn{
      appearance:none;
      border:1px solid var(--stroke);
      background: #f0ede6;
      color: var(--text);
      padding: 10px 12px;
      border-radius: 12px;
      cursor:pointer;
      font-weight: 650;
      font-size: 13px;
      letter-spacing:.2px;
      transition: .15s ease;
      display:inline-flex;
      align-items:center;
      gap: 8px;
      user-select:none;
    }
    .btn:hover{ transform: translateY(-1px); border-color: var(--accent); }
    .btn:active{ transform: translateY(0px); }
    .btn:disabled{
      opacity:.6;
      cursor:not-allowed;
      transform:none;
      background: #e4dfd6;
      color: #8a8378;
      border-color: #d5cfc4;
    }

    .btn.primary{
      border: none;
      background: linear-gradient(90deg, var(--accent), var(--accent-strong));
      color: #ffffff;
      box-shadow: 0 8px 22px rgba(79,113,80,.28);
    }
    .btn.primary:hover{
      box-shadow: 0 10px 28px rgba(79,113,80,.32);
    }
    .btn.success{
      background: linear-gradient(90deg, var(--good), #2a6f4c);
      box-shadow: 0 10px 28px rgba(42,111,76,.32);
    }

    .btn.ghost{
      background: #f7f3ec;
    }

    input[type=file]{
      position:absolute;
      inset:0;
      opacity:0;
      pointer-events:none; /* <-- IMPORTANT: non intercetta click */
    }

    .progress{
      margin-top: 12px;
      height: 10px;
      border-radius: 999px;
      background: #e6dfd3;
      border: 1px solid #d9d2c7;
      overflow: hidden;
    }

    .bar{
      width: 0%;
      height: 100%;
      border-radius: 999px;
      background: linear-gradient(90deg, #88a17f, var(--accent));
      transition: width .2s ease;
    }

    .bar.indet{
      width: 100%;
      animation: indet 1.2s infinite linear;
      transform-origin:left;
    }
    @keyframes indet{
      0%{ transform: translateX(-100%); }
      100%{ transform: translateX(100%); }
    }

    .stepper{
      margin-top: 14px;
      display:grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }
    .step{
      border: 1px solid #d7d1c7;
      background: #faf7f2;
      border-radius: 999px;
      padding: 10px 12px;
      color: var(--muted2);
      font-size: 12px;
      display:flex;
      align-items:center;
      gap: 10px;
      min-height: 44px;
    }
    .badge{
      width: 22px;height:22px;border-radius: 8px;
      display:flex;align-items:center;justify-content:center;
      border: 1px solid #d7d1c7;
      background: #ede6db;
      color: var(--muted);
      font-weight: 750;
      font-size: 12px;
      flex: 0 0 auto;
    }
    .step.active{
      color: var(--text);
      border-color: rgba(107,139,95,.6);
      background: rgba(107,139,95,.12);
    }
    .step.done{
      color: #244c38;
      border-color: rgba(47,133,90,.35);
      background: rgba(47,133,90,.12);
    }
    .step.done .badge{
      border-color: rgba(47,133,90,.40);
      background: rgba(47,133,90,.12);
      color: #244c38;
    }

    .statusbox{
      margin-top: 14px;
      border-radius: 14px;
      padding: 12px;
      border: 1px solid #dcd4c9;
      background: #f9f7f2;
    }
    .statusline{
      display:flex;
      align-items:flex-start;
      gap: 10px;
      font-size: 13px;
      color: var(--muted);
      white-space: pre-wrap;
      line-height: 1.45;
    }
    .icon{
      width: 18px;height:18px; flex: 0 0 auto;
      margin-top: 1px;
      opacity: .95;
    }

    .ok{ color: #1f2f20; }
    .err{ color: #2d1c1c; }
    .kpi{
      margin-top: 10px;
      display:flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    .chip{
      border: 1px solid #d7d1c7;
      background: #f1ece4;
      border-radius: 999px;
      padding: 7px 10px;
      font-size: 12px;
      color: var(--muted);
      display:inline-flex;
      align-items:center;
      gap: 8px;
    }
    .chip strong{ color: var(--text); font-weight: 750; }

    .footerhint{
      margin-top: 12px;
      color: var(--muted2);
      font-size: 12px;
    }

    .tiny{
      font-size: 11.5px;
      color: var(--muted2);
    }

    .recap{
      margin-top: 18px;
    }
    .recap h3{
      margin: 0 0 8px;
      font-size: 15px;
      letter-spacing: .2px;
    }
    .recap-sub{
      margin: 0 0 12px;
      color: var(--muted2);
      font-size: 12px;
    }
    .recap-grid{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
      width: 100%;
    }
    .recap-card{
      border: 1px solid #e0d7c9;
      background: #f9f6f1;
      border-radius: 14px;
      padding: 12px;
      display: flex;
      flex-direction: column;
      gap: 6px;
      max-width: 100%;
    }
    .recap-row{
      display: grid;
      grid-template-columns: 140px 1fr;
      gap: 6px;
      align-items: start;
      font-size: 13px;
      color: var(--text);
      max-width: 100%;
    }
    .recap-label{
      color: var(--muted2);
      text-transform: uppercase;
      letter-spacing: .2px;
      font-size: 11px;
      font-weight: 700;
      white-space: nowrap;
    }
    .recap-value{
      overflow-wrap: anywhere;
      word-break: break-word;
      color: var(--text);
    }
    .recap-value.desc{
      display: block;
      white-space: normal;
      overflow: visible;
    }
    .recap-actions{
      margin-top: 10px;
    }

    a.link{
      color: var(--accent-strong);
      text-decoration: none;
      border-bottom: 1px dashed rgba(79,113,80,.55);
    }
    a.link:hover{ opacity:.92; }

    @media (max-width: 720px){
      .wrap{ padding: 0 18px; }
      .dzrow{ align-items:flex-start; }
      .actions{ width:100%; }
    }

    @media (max-width: 620px){
      .dzrow{ flex-direction: column; align-items: stretch; }
      .actions{ justify-content:flex-start; }
    }

    @media (max-width: 520px){
      .recap-row{
        grid-template-columns: 1fr;
      }
    }

    @media (max-width: 720px){
      .stepper{ grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 420px){
      .stepper{
        display:flex;
        flex-direction: column;
      }
      .step{ border-radius: 12px; }
      .wrap{ padding: 0 16px; }
      .dropzone{ padding: 14px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="brand">
        <img class="logo" alt="Adeptic"
          src="data:image/png;base64,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" />
        <div>
          <h1>Horizon Call Extractor</h1>
          <p>Upload a Horizon Europe PDF → extract calls/topics → download the Excel.</p>
        </div>
      </div>
    </div>

    <div class="card">
      <h2>Upload PDF</h2>
      <p class="sub">
        Upload a Horizon Europe PDF to extract calls/topics and generate an Excel file.
      </p>

      <div class="filters">
        <div class="filter">
          <div class="filterlabel">Call types</div>
          <div class="multiselect" id="typeBox">
            <button class="btn selectbtn" id="typeBtn" type="button">
              <span id="typeSummary">All types</span>
              <span class="chev">▾</span>
            </button>
            <div class="dropdown" id="typeMenu">
              <div class="menuhead">
                <span>Select one or more types (default: all)</span>
                <div class="menuactions">
                  <button class="chipbtn" type="button" id="typeAll">Select all</button>
                  <button class="chipbtn" type="button" id="typeNone">Select none</button>
                </div>
              </div>
              <div class="checklist" id="typeList"></div>
            </div>
          </div>
        </div>
        <div class="filter">
          <div class="filterlabel">Min. budget per project</div>
          <div class="sliderwrap">
            <div class="bubble" id="budgetBubble">0 M€</div>
            <input type="range" id="budget" min="0" max="15" step="1" value="0" aria-label="Minimum budget per project in million euros"/>
            <div class="sliderlabels"><span>0</span><span>15+ M€</span></div>
          </div>
        </div>
        <div class="filter">
          <div class="filterlabel">Opening date</div>
          <input class="textinput" id="openFilter" type="text" placeholder="e.g. 2026 or 2026-Q1 or 2026-05" aria-label="Filter by opening date"/>
          <div class="filterhint">Year, quarter (YYYY-Q1), month (YYYY-MM), exact day (YYYY-MM-DD), or text dates (e.g. 23 Sep 2026). Upper bound: dates up to the end of the period.</div>
        </div>
        <div class="filter">
          <div class="filterlabel">Deadline date</div>
          <input class="textinput" id="deadlineFilter" type="text" placeholder="e.g. 2027 or 2027-Q4 or 2027-11-15" aria-label="Filter by deadline date"/>
          <div class="filterhint">Year, quarter (YYYY-Qx), month (YYYY-MM), exact day (YYYY-MM-DD), or text dates (e.g. 23 Sep 2026). Year/period filters return all deadlines on or before the end of that period.</div>
        </div>
      </div>

      <div class="dropzone" id="dz">
        <input id="file" type="file" accept="application/pdf" />
        <div class="dzrow">
          <div class="filemeta">
            <div class="filename" id="filename">No file selected</div>
            <div class="filesub" id="filesub">Supported format: PDF</div>
          </div>
          <div class="actions">
            <button class="btn ghost" id="clear" disabled>Remove</button>
            <button class="btn primary" id="go" disabled>Generate Excel</button>
          </div>
        </div>

        <div class="progress" aria-label="progress">
          <div class="bar" id="bar"></div>
        </div>

        <div class="stepper" aria-label="steps">
          <div class="step" id="s1"><span class="badge">1</span><span>Presign URL</span></div>
          <div class="step" id="s2"><span class="badge">2</span><span>Upload to S3</span></div>
          <div class="step" id="s3"><span class="badge">3</span><span>Parsing + Excel</span></div>
          <div class="step" id="s4"><span class="badge">4</span><span>Download link</span></div>
        </div>

        <div class="statusbox">
          <div class="statusline" id="status">
            <svg class="icon" viewBox="0 0 24 24" fill="none" aria-hidden="true">
              <path d="M12 2a10 10 0 1 0 0 20 10 10 0 0 0 0-20Z" stroke="rgba(46,41,35,.45)" stroke-width="1.5"/>
              <path d="M12 8v5" stroke="rgba(46,41,35,.65)" stroke-width="1.5" stroke-linecap="round"/>
              <path d="M12 16h.01" stroke="rgba(46,41,35,.75)" stroke-width="2.5" stroke-linecap="round"/>
            </svg>
            <div>
              <div class="ok" id="statusMain">Select a PDF to get started.</div>
              <div class="tiny" id="statusSub">Tip: large files may take a few seconds to process.</div>
            </div>
          </div>

          <div class="kpi" id="kpi" style="display:none;">
            <div class="chip">Rows extracted: <strong id="rows">0</strong></div>
            <div class="chip">Status: <strong id="final">Ready</strong></div>
            <button class="btn primary" id="downloadBtn" style="display:none;">Download Excel</button>
          </div>

        </div>
    </div>
  </div>

  <div class="recap card" id="recapWrap" style="display:none;">
    <h3>Recap</h3>
    <p class="recap-sub">After generation, the first topics are shown below with key details (ID, title, action/type, budget, opening/deadline).</p>
    <div class="recap-grid" id="recapGrid"></div>
    <div class="recap-actions">
      <button class="btn ghost" type="button" id="recapMore" style="display:none;">Show more</button>
    </div>
  </div>
</div>

<script>
const $ = (id) => document.getElementById(id);

const ACTION_TYPES = ["RIA","IA","CSA","PCP","PPI","COFUND","ERC","MSCA","EIC-PATHFINDER","EIC-TRANSITION","EIC-ACCELERATOR"];

const state = {
  file: null,
  downloadUrl: null,
  selectedTypes: new Set(), // empty = all types (no filter)
  minBudget: 0,
  openingFilter: "",
  deadlineFilter: "",
  recapRows: [],
  showAllRecap: false,
};

function fmtBytes(bytes){
  const u = ["B","KB","MB","GB"];
  let i = 0;
  let n = bytes;
  while(n >= 1024 && i < u.length-1){ n/=1024; i++; }
  return (i === 0 ? n : n.toFixed(1)) + " " + u[i];
}

function updateTypeSummary(){
  const size = state.selectedTypes.size;
  let summary = "All types";
  if(size === ACTION_TYPES.length){
    summary = "All types";
  } else if(size === 1){
    summary = Array.from(state.selectedTypes)[0];
  } else if(size > 1){
    const first = Array.from(state.selectedTypes)[0];
    summary = `${first} +${size-1}`;
  } else {
    summary = "All types";
  }
  $("typeSummary").textContent = summary;
}

function syncTypeCheckboxes(){
  document.querySelectorAll("#typeList input[type=checkbox]").forEach(inp => {
    inp.checked = state.selectedTypes.has(inp.value);
  });
  updateTypeSummary();
}

function renderTypeList(){
  const box = $("typeList");
  box.innerHTML = "";
  ACTION_TYPES.forEach(t => {
    const lbl = document.createElement("label");
    lbl.className = "checkrow";
    const input = document.createElement("input");
    input.type = "checkbox";
    input.value = t;
    input.checked = state.selectedTypes.has(t);
    input.onchange = () => {
      if(input.checked){
        state.selectedTypes.add(t);
      } else {
        state.selectedTypes.delete(t);
      }
      syncTypeCheckboxes();
    };
    const span = document.createElement("span");
    span.textContent = t;
    lbl.appendChild(input);
    lbl.appendChild(span);
    box.appendChild(lbl);
  });
}

function formatBudget(val){
  if(val >= 15){
    return "15+ M€";
  }
  return `${val} M€`;
}

function setBudget(val){
  const num = Math.min(15, Math.max(0, Math.round(Number(val) || 0)));
  state.minBudget = num;
  $("budget").value = String(num);
  $("budgetBubble").textContent = formatBudget(num);
}

function setFiltersDisabled(disabled){
  $("typeBtn").disabled = disabled;
  $("typeAll").disabled = disabled;
  $("typeNone").disabled = disabled;
  document.querySelectorAll("#typeList input[type=checkbox]").forEach(inp => inp.disabled = disabled);
  $("budget").disabled = disabled;
  $("openFilter").disabled = disabled;
  $("deadlineFilter").disabled = disabled;
  if(disabled){
    $("typeMenu").classList.remove("open");
  }
}

function setSteps(activeIdx, doneBefore=false){
  const steps = [$("s1"),$("s2"),$("s3"),$("s4")];
  steps.forEach((el, idx) => {
    el.classList.remove("active","done");
    if(doneBefore && idx < activeIdx) el.classList.add("done");
    if(idx === activeIdx) el.classList.add("active");
  });
}

function clearSteps(){
  [$("s1"),$("s2"),$("s3"),$("s4")].forEach(el => el.classList.remove("active","done"));
}

function setStatus(main, sub, isError=false){
  $("statusMain").textContent = main;
  $("statusSub").textContent = sub || "";
  if(isError){
    $("final").textContent = "ERROR";
    $("final").style.color = "var(--bad)";
  } else {
    $("final").style.color = "var(--text)";
  }
}

function setBusy(busy){
  $("go").disabled = busy || !state.file;
  $("clear").disabled = busy || !state.file;
  $("file").disabled = busy;
  setFiltersDisabled(busy);
}

function barNone(){
  $("bar").classList.remove("indet");
  $("bar").style.width = "0%";
}
function barIndeterminate(){
  $("bar").style.width = "100%";
  $("bar").classList.add("indet");
}
function barSet(pct){
  $("bar").classList.remove("indet");
  $("bar").style.width = pct + "%";
}

async function fetchJson(url, opts){
  const res = await fetch(url, opts);
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch(e) {}
  if(!res.ok){
    const msg = (data && (data.message || data.error)) ? (data.message || data.error) : (text || ("HTTP " + res.status));
    const err = new Error(msg);
    err.status = res.status;
    err.payload = data;
    throw err;
  }
  return data;
}

function formatRecapBudget(val){
  const num = Number(val);
  if(Number.isNaN(num)){
    return "—";
  }
  if(num >= 15){
    return "15+ M€";
  }
  return `${num} M€`;
}

function topicLink(tid, url){
  if(url) return url;
  const clean = (tid || "").trim();
  if(!clean) return "";
  return `https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-details/${clean}`;
}

function renderRecap(){
  const wrap = $("recapWrap");
  const grid = $("recapGrid");
  const btn = $("recapMore");
  if(!wrap || !grid || !btn) return;

  grid.innerHTML = "";
  const rows = state.recapRows || [];
  const limit = 120;
  const visible = state.showAllRecap ? rows : rows.slice(0, limit);

  visible.forEach((r) => {
    const card = document.createElement("div");
    card.className = "recap-card";

    const makeRow = (label, valueEl) => {
      const row = document.createElement("div");
      row.className = "recap-row";
      const l = document.createElement("div");
      l.className = "recap-label";
      l.textContent = label;
      const v = document.createElement("div");
      v.className = "recap-value";
      if(valueEl){
        v.appendChild(valueEl);
      } else {
        v.textContent = "—";
      }
      row.appendChild(l);
      row.appendChild(v);
      return row;
    };

    const topicLinkEl = document.createElement("a");
    topicLinkEl.href = topicLink(r.topic_id, r.topic_url) || "#";
    topicLinkEl.target = "_blank";
    topicLinkEl.rel = "noreferrer";
    topicLinkEl.textContent = r.topic_id || "—";
    topicLinkEl.className = "link";
    card.appendChild(makeRow("Topic ID", topicLinkEl));

    const actionEl = document.createElement("span");
    actionEl.textContent = r.action_type || "—";
    card.appendChild(makeRow("Action type", actionEl));

    const typeEl = document.createElement("span");
    typeEl.textContent = r.type || "—";
    card.appendChild(makeRow("Type", typeEl));

    const descEl = document.createElement("div");
    descEl.className = "recap-value desc";
    descEl.textContent = r.topic_title || "—";
    const descRow = document.createElement("div");
    descRow.className = "recap-row";
    const descLabel = document.createElement("div");
    descLabel.className = "recap-label";
    descLabel.textContent = "Topic title";
    descRow.appendChild(descLabel);
    descRow.appendChild(descEl);
    card.appendChild(descRow);

    const budgetEl = document.createElement("span");
    budgetEl.textContent = formatRecapBudget(r.budget_per_project_min_eur_m);
    card.appendChild(makeRow("Min budget per project (M€)", budgetEl));

    const openEl = document.createElement("span");
    openEl.textContent = r.opening_date || "—";
    card.appendChild(makeRow("Opening date", openEl));

    const deadlineEl = document.createElement("span");
    deadlineEl.textContent = r.deadline_date || "—";
    card.appendChild(makeRow("Deadline date", deadlineEl));

    grid.appendChild(card);
  });

  const hidden = rows.length - visible.length;
  if(hidden > 0){
    btn.style.display = "inline-flex";
    btn.textContent = state.showAllRecap ? "Show less" : `Show more (${hidden})`;
  } else {
    btn.style.display = "none";
  }

  wrap.style.display = rows.length ? "block" : "none";
}

function showResult(rowsData, downloadUrl){
  $("kpi").style.display = "flex";
  const count = Array.isArray(rowsData) ? rowsData.length : (rowsData ?? 0);
  $("rows").textContent = String(count);
  $("final").textContent = "Ready";
  const dlBtn = $("downloadBtn");
  dlBtn.style.display = downloadUrl ? "inline-flex" : "none";
  if(downloadUrl){
    dlBtn.classList.add("success");
  }

  state.recapRows = Array.isArray(rowsData) ? rowsData : [];
  state.showAllRecap = false;
  renderRecap();

  const recap = $("recapWrap");
  if(recap && recap.style.display !== "none"){
    recap.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

function resetResult(){
  $("kpi").style.display = "none";
  $("downloadBtn").style.display = "none";
  $("downloadBtn").classList.remove("success");
  state.downloadUrl = null;
  state.recapRows = [];
  state.showAllRecap = false;
  renderRecap();
}

function setFile(f){
  state.file = f || null;
  resetResult();
  if(!state.file){
    $("filename").textContent = "No file selected";
    $("filesub").textContent = "Supported format: PDF";
    setStatus("Select a PDF to get started.", "Drag & drop or click to select.");
    clearSteps();
    barNone();
    setBusy(false);
    return;
  }
  $("filename").textContent = state.file.name;
  $("filesub").textContent = fmtBytes(state.file.size) + " • " + (state.file.type || "application/pdf");
  setStatus("Ready to generate Excel.", "Click “Generate Excel” to continue.");
  setBusy(false);
}

function isPdfFile(f){
  if(!f) return false;
  // Some browsers do not set type; also check the extension
  const nameOk = (f.name || "").toLowerCase().endsWith(".pdf");
  const typeOk = (f.type || "") === "application/pdf";
  return typeOk || nameOk;
}

// Dropzone UX
const dz = $("dz");
dz.addEventListener("click", (e) => {
  // if clicking a button, do not trigger the picker
  if (e.target.closest("button")) return;
  $("file").click();
});
["dragenter","dragover"].forEach(evt => dz.addEventListener(evt, (e) => {
  e.preventDefault(); e.stopPropagation();
  dz.classList.add("drag");
}));
["dragleave","drop"].forEach(evt => dz.addEventListener(evt, (e) => {
  e.preventDefault(); e.stopPropagation();
  dz.classList.remove("drag");
}));
dz.addEventListener("drop", (e) => {
  const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
  if(!f) return;
  if(!isPdfFile(f)){
    setStatus("Invalid format.", "Select a PDF file (.pdf extension).", true);
    return;
  }
  $("file").value = "";
  setFile(f);
});

// File picker
$("file").addEventListener("change", () => {
  const f = $("file").files[0];
  if(!f) return;
  if(!isPdfFile(f)){
    setStatus("Invalid format.", "Select a PDF file (.pdf extension).", true);
    $("file").value = "";
    return;
  }
  setFile(f);
});

$("clear").onclick = () => {
  $("file").value = "";
  setFile(null);
};

$("typeBtn").onclick = (e) => {
  e.stopPropagation();
  $("typeMenu").classList.toggle("open");
};
$("typeAll").onclick = () => {
  state.selectedTypes = new Set(ACTION_TYPES);
  syncTypeCheckboxes();
};
$("typeNone").onclick = () => {
  state.selectedTypes = new Set();
  syncTypeCheckboxes();
};
document.addEventListener("click", (e) => {
  if(!$("typeBox").contains(e.target)){
    $("typeMenu").classList.remove("open");
  }
});

$("budget").addEventListener("input", (e) => setBudget(e.target.value));
$("openFilter").addEventListener("input", (e) => {
  state.openingFilter = (e.target.value || "").trim();
});
$("deadlineFilter").addEventListener("input", (e) => {
  state.deadlineFilter = (e.target.value || "").trim();
});

$("downloadBtn").onclick = () => {
  if(state.downloadUrl) window.location = state.downloadUrl;
};

$("recapMore").onclick = () => {
  state.showAllRecap = !state.showAllRecap;
  renderRecap();
};

$("go").onclick = async () => {
  const f = state.file;
  if(!f){
    setStatus("Select a PDF.", "Drag & drop or click to select.", true);
    return;
  }

  setBusy(true);
  resetResult();
  barIndeterminate();

  try {
    // 1) Presign
    setSteps(0, false);
    setStatus("1/4 • Presign URL", "Requesting a time-limited S3 upload link.");
    const pres = await fetchJson("/presign");

    // 2) Upload S3 (fetch PUT: no Content-Type, keep the fix)
    setSteps(1, true);
    setStatus("2/4 • Uploading to S3…", "Sending the PDF via the presigned URL.");
    // (fetch does not expose native progress without XHR; use indeterminate)
    const put = await fetch(pres.upload_url, { method: "PUT", body: f });
    if(!put.ok){
      throw new Error("Upload failed: HTTP " + put.status);
    }

    // 3) Process
    setSteps(2, true);
    setStatus("3/4 • Parsing + generating Excel…", "Extracting calls/topics and building the spreadsheet.");
    const proc = await fetchJson("/process", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        pdf_key: pres.pdf_key,
        action_types: Array.from(state.selectedTypes),
        min_budget_m: state.minBudget,
        original_name: state.file?.name || "",
        opening_filter: state.openingFilter,
        deadline_filter: state.deadlineFilter,
      })
    });

    // 4) Download presigned
    setSteps(3, true);
    setStatus("4/4 • Preparing download…", "Creating a secure temporary download link.");
    const dl = await fetchJson("/download", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ excel_key: proc.excel_key })
    });

    state.downloadUrl = dl.download_url;

    barSet(100);
    [$("s1"),$("s2"),$("s3"),$("s4")].forEach(el => { el.classList.remove("active"); el.classList.add("done"); });

    const rows = (proc && proc.rows) ? proc.rows : [];
    showResult(rows, state.downloadUrl);

    setStatus("Completed ✅", "Download is ready. Use “Download Excel” to get the file.");

  } catch (e) {
    barNone();
    clearSteps();

    const msg = (e && e.message) ? e.message : String(e);
    let sub = "Try again. If it persists, check S3 CORS / bucket permissions / Lambda logs.";
    if(e && e.status){
      sub = "Details: HTTP " + e.status + ". " + sub;
    }
    setStatus("Error during processing.", msg + "\\n" + sub, true);
  } finally {
    setBusy(false);
  }
};

// init
renderTypeList();
syncTypeCheckboxes();
setBudget(state.minBudget);
setFile(null);
</script>
</body>
</html>
"""


def _resp(status_code: int, body: str, content_type: str = "application/json"):
    return {
        "statusCode": status_code,
        "headers": {
            "content-type": content_type,
            "access-control-allow-origin": "*",
            "access-control-allow-methods": "GET,POST,OPTIONS",
            "access-control-allow-headers": "content-type",
        },
        "body": body,
    }


def _json(obj):
    return json.dumps(obj, ensure_ascii=False)


def _coerce_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def handler(event, context):
    try:
        # Supporta invocazioni "dirette" (CLI) e HTTP (Lambda URL)
        if "requestContext" not in event:
            key = event["pdf_key"]
            action_types = event.get("action_types")
            min_budget_m = _coerce_float(event.get("min_budget_m"))
            if min_budget_m is None:
                min_budget_m = DEFAULT_MIN_BUDGET_M
            original_name = event.get("original_name") or ""
            opening_filter = event.get("opening_filter") or ""
            deadline_filter = event.get("deadline_filter") or ""
            return _process_pdf_key(
                key,
                context=context,
                action_types=action_types,
                min_budget_m=min_budget_m,
                opening_filter=opening_filter,
                deadline_filter=deadline_filter,
                original_name=original_name,
            )

        method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
        path = event.get("rawPath", "/")

        if method == "OPTIONS":
            return _resp(200, "")

        if method == "GET" and path == "/":
            return _resp(200, HTML, content_type="text/html; charset=utf-8")

        if method == "GET" and path == "/presign":
            _require_bucket()
            pdf_key = f"uploads/{uuid.uuid4()}.pdf"
            upload_url = s3.generate_presigned_url(
                "put_object",
                Params={"Bucket": BUCKET, "Key": pdf_key},
                ExpiresIn=900,
            )
            return _resp(200, _json({"upload_url": upload_url, "pdf_key": pdf_key}))

        if method == "POST" and path == "/process":
            _require_bucket()
            body = event.get("body") or "{}"
            data = json.loads(body)
            pdf_key = data["pdf_key"]
            action_types = data.get("action_types")
            min_budget_m = _coerce_float(data.get("min_budget_m"))
            if min_budget_m is None:
                min_budget_m = DEFAULT_MIN_BUDGET_M
            original_name = data.get("original_name") or ""
            opening_filter = data.get("opening_filter") or ""
            deadline_filter = data.get("deadline_filter") or ""
            result = _process_pdf_key(
                pdf_key,
                context=context,
                action_types=action_types,
                min_budget_m=min_budget_m,
                opening_filter=opening_filter,
                deadline_filter=deadline_filter,
                original_name=original_name,
            )
            return _resp(200, _json(result))

        if method == "POST" and path == "/download":
            _require_bucket()
            body = event.get("body") or "{}"
            data = json.loads(body)
            excel_key = data["excel_key"]
            download_url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": BUCKET, "Key": excel_key},
                ExpiresIn=900,
            )
            return _resp(200, _json({"download_url": download_url}))

        return _resp(404, _json({"error": "not_found", "path": path, "method": method}))

    except Exception as e:
        # log completo su CloudWatch, ma rispondiamo con messaggio leggibile al browser
        print("ERROR:", repr(e))
        print(traceback.format_exc())

        return _resp(
          500,
          _json({
          "error": "internal",
          "message": str(e),
          "type": e.__class__.__name__,
          "request_id": getattr(context, "aws_request_id", None),
          }),
        )


def _process_pdf_key(
    key: str,
    context=None,
    action_types=None,
    min_budget_m=None,
    opening_filter: str = "",
    deadline_filter: str = "",
    original_name: str = "",
):
    _require_bucket()
    if min_budget_m is None:
        min_budget_m = DEFAULT_MIN_BUDGET_M

    local_pdf = f"/tmp/{uuid.uuid4()}.pdf"
    local_xlsx = f"/tmp/{uuid.uuid4()}.xlsx"

    s3.download_file(BUCKET, key, local_pdf)

    text = extract_text(local_pdf)
    rows = parse_calls(text)

    for r in rows:
        derived_budget = _compute_budget_per_project_m(r)
        r["budget_per_project_min_eur_m"] = derived_budget
        r["budget_per_project_m"] = derived_budget

    rows = filter_rows(
        rows,
        action_types=action_types,
        min_budget_m=min_budget_m,
        opening_filter=opening_filter,
        deadline_filter=deadline_filter,
    )

    # --- OpenAI descriptions (optional) ---
    # Regola anti-timeout: se mancano < 8s, stop OpenAI.
    desc_cache: dict = {}
    if OPENAI_API_KEY:
        n = 0
        for r in rows:
            if n >= OPENAI_MAX_TOPICS:
                break

            # time budget
            if context is not None:
                remaining_ms = context.get_remaining_time_in_millis()
                if remaining_ms is not None and remaining_ms < 8000:
                    print(f"Stopping OpenAI: low remaining time {remaining_ms}ms")
                    break

            body = (r.get("topic_body") or "").strip()
            if not body:
                continue

            cur = (r.get("topic_description") or "").strip()
            if len(cur) >= 80:
                continue

            desc = _openai_topic_description(
                topic_id=r.get("topic_id") or "",
                topic_title=r.get("topic_title") or "",
                body_text=body,
                cache=desc_cache,
            )
            if desc:
                r["topic_description"] = desc
                n += 1
            else:
                r["topic_description"] = ""

    for r in rows:
        if r.get("topic_description") is None:
            r["topic_description"] = ""

        if r.get("budget_per_project_min_eur_m") is None:
            derived_budget = _compute_budget_per_project_m(r)
            r["budget_per_project_min_eur_m"] = derived_budget
            r["budget_per_project_m"] = derived_budget

    # Don't export topic_body to Excel
    for r in rows:
        r.pop("topic_body", None)

    write_xlsx(rows, local_xlsx)

    safe_base = _safe_base_name(original_name) or "file"
    out_key = f"outputs/{uuid.uuid4()}/{safe_base}.xlsx"
    s3.upload_file(local_xlsx, BUCKET, out_key)

    recap_rows = []
    for r in rows:
        recap_rows.append(
            {
                "topic_id": r.get("topic_id"),
                "topic_url": _topic_url(r.get("topic_id")),
                "topic_title": r.get("topic_title") or "",
                "action_type": r.get("action_type"),
                "type": r.get("stage") or r.get("call_round"),
                "budget_per_project_min_eur_m": r.get("budget_per_project_min_eur_m"),
                "opening_date": r.get("opening_date"),
                "deadline_date": r.get("deadline_date"),
            }
        )

    return {"status": "ok", "excel_key": out_key, "rows": recap_rows, "rows_count": len(rows)}

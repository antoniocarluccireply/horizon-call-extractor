import os
import re
import uuid
import json
import calendar
import boto3
import urllib.error
import urllib.request
import traceback
from datetime import date
from typing import Optional
from pypdf import PdfReader
from openpyxl import Workbook

from parser_horizon import parse_calls
from parser_edf import parse_edf_calls

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


HORIZON_PATTERNS = (
    re.compile(r"\bHORIZON-[A-Z0-9]+-\d{4}-\d{2}", flags=re.IGNORECASE),
    re.compile(r"Horizon Europe\s*-\s*Work Programme", flags=re.IGNORECASE),
)

EDF_PATTERNS = (
    re.compile(r"\bEDF-\d{4}-", flags=re.IGNORECASE),
    re.compile(r"European Defence Fund", flags=re.IGNORECASE),
    re.compile(r"Commission Implementing Decision", flags=re.IGNORECASE),
)


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


def _detect_document_type(text: str) -> str:
    snippet = (text or "").strip()
    for pat in HORIZON_PATTERNS:
        if pat.search(snippet):
            return "horizon"
    for pat in EDF_PATTERNS:
        if pat.search(snippet):
            return "edf"
    return ""


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


def write_horizon_xlsx(rows, xlsx_path: str):
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


def write_edf_xlsx(rows, xlsx_path: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "edf_topics"

    headers = [
        "call_id",
        "topic_id",
        "topic_title",
        "type_of_action",
        "indicative_budget_eur_m",
        "number_of_actions_to_be_funded",
        "step_flag",
        "page",
    ]
    ws.append(headers)

    for r in rows:
        ws.append([r.get(h) for h in headers])

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


def filter_edf_rows(
    rows,
    call_families=None,
    min_budget_m: float = None,
    max_budget_m: float = None,
    step_only: bool = False,
    title_query: str = "",
):
    allowed = None
    if call_families:
        allowed = {str(t).upper() for t in call_families if str(t).strip()}
        if not allowed:
            allowed = None

    q = (title_query or "").strip().lower()

    filtered = []
    for r in rows:
        if allowed is not None:
            call_id = (r.get("call_id") or "").upper()
            family = call_id.split("-")[2] if "-" in call_id else call_id
            if family not in allowed:
                continue

        budget_val = r.get("indicative_budget_eur_m")
        try:
            budget_val = float(budget_val) if budget_val is not None else None
        except (TypeError, ValueError):
            budget_val = None

        if min_budget_m is not None and budget_val is not None and budget_val < min_budget_m:
            continue
        if max_budget_m is not None and budget_val is not None and budget_val > max_budget_m:
            continue

        if step_only and not str(r.get("step_flag") or "").upper().startswith("STEP"):
            continue

        if q and q not in (r.get("topic_title") or "").lower():
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
<html lang=\\"en\\">
<head>
  <meta charset=\\"utf-8\\" />
  <meta name=\\"viewport\\" content=\\"width=device-width, initial-scale=1\\" />
  <title>Horizon & EDF Call Extractor</title>
  <style>
    :root{
      --bg:#0f172a;
      --card:#0b1222;
      --muted:#8ea5c3;
      --text:#e8edf5;
      --accent:#f0b429;
      --accent-2:#38bdf8;
      --danger:#f87171;
      --good:#22c55e;
      --border:rgba(255,255,255,0.08);
      --radius:14px;
    }
    *{box-sizing:border-box;}
    body{
      margin:0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, \\"Segoe UI\\", sans-serif;
      background: radial-gradient(circle at 18% 22%, rgba(56,189,248,.18), transparent 30%), radial-gradient(circle at 86% 12%, rgba(240,180,41,.16), transparent 32%), #0b1020;
      color:var(--text);
    }
    .shell{max-width:1080px;margin:24px auto 60px;padding:0 18px;}
    .hero{padding:16px 18px 12px;border:1px solid var(--border);border-radius:18px;background:rgba(255,255,255,0.02);backdrop-filter: blur(10px);}
    .hero h1{margin:0;font-size:22px;}
    .hero p{margin:6px 0 0;color:var(--muted);font-size:14px;}
    .tabs{display:flex;gap:10px;margin:16px 0;}
    .tabbtn{flex:1 1 0;border:1px solid var(--border);border-radius:14px;padding:12px 14px;cursor:pointer;background:var(--card);color:var(--text);display:flex;align-items:center;justify-content:space-between;font-weight:700;letter-spacing:.2px;}
    .tabbtn.active{border-color:var(--accent);box-shadow:0 10px 28px rgba(240,180,41,.2);}
    .pill{font-size:12px;color:var(--card);background:var(--accent);padding:4px 9px;border-radius:999px;}
    .card{border:1px solid var(--border);border-radius:var(--radius);padding:16px 16px 18px;background:rgba(255,255,255,0.03);box-shadow:0 16px 40px rgba(0,0,0,.3);}
    .card h2{margin:0 0 6px;font-size:16px;}
    .sub{margin:0 0 10px;color:var(--muted);font-size:13px;line-height:1.5;}
    .stack{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px;margin-top:12px;}
    .label{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.2px;color:var(--muted);margin-bottom:6px;}
    .input,select{width:100%;padding:10px 12px;border-radius:12px;border:1px solid var(--border);background:rgba(255,255,255,0.02);color:var(--text);font-size:14px;}
    .row{display:flex;align-items:center;gap:10px;flex-wrap:wrap;}
    .btn{border:1px solid var(--border);background:linear-gradient(90deg,#1e293b,#0f172a);color:var(--text);border-radius:12px;padding:10px 12px;font-weight:700;cursor:pointer;display:inline-flex;gap:8px;align-items:center;}
    .btn.primary{background:linear-gradient(90deg,var(--accent),#f59e0b);color:#0b0d14;border:none;}
    .btn.ghost{background:rgba(255,255,255,0.03);}
    .btn:disabled{opacity:.5;cursor:not-allowed;}
    .drop{border:1px dashed var(--border);border-radius:16px;padding:14px;position:relative;background:rgba(255,255,255,0.02);}
    .drop.drag{border-color:var(--accent);}
    .filelist{display:flex;flex-direction:column;gap:6px;margin-top:6px;}
    .filechip{border:1px solid var(--border);border-radius:12px;padding:8px 10px;display:flex;align-items:center;justify-content:space-between;gap:8px;background:rgba(255,255,255,0.04);word-break:break-word;}
    .hint{color:var(--muted);font-size:12px;margin-top:4px;}
    table{width:100%;border-collapse:collapse;margin-top:10px;}
    th,td{border:1px solid var(--border);padding:10px 8px;text-align:left;font-size:13px;}
    th{color:var(--muted);font-weight:700;}
    .table-wrap{overflow:auto;max-height:460px;}
    @media(max-width:720px){
      table,thead,tbody,th,td,tr{display:block;}
      th{display:none;}
      td{border:none;border-bottom:1px solid var(--border);padding:8px 6px;}
      td::before{content:attr(data-label);display:block;font-size:12px;color:var(--muted);text-transform:uppercase;}
    }
    .status{border:1px solid var(--border);padding:10px 12px;border-radius:12px;background:rgba(255,255,255,0.02);margin-top:10px;}
    .status strong{color:var(--accent);}
    .badge{padding:4px 8px;border-radius:10px;background:rgba(255,255,255,0.06);color:var(--muted);font-size:12px;}
    .error{color:var(--danger);}
    .success{color:var(--good);}
    .grid-two{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:8px;margin-top:8px;}
    .download{display:flex;align-items:center;gap:8px;margin-top:8px;}
    .hidden{display:none;}
  </style>
</head>
<body>
  <div class=\\"shell\\">
    <div class=\\"hero\\">
      <h1>Horizon & EDF Call Extractor</h1>
      <p>Auto-detects document family, extracts topics, shows a responsive table, and generates Excel on demand.</p>
    </div>

    <div class=\\"tabs\\">
      <button class=\\"tabbtn active\\" id=\\"tab-horizon\\">Horizon <span class=\\"pill\\">Work Programme</span></button>
      <button class=\\"tabbtn\\" id=\\"tab-edf\\">EDF <span class=\\"pill\\" style=\\"background:var(--accent-2);color:#0b1020;\\">Call topic descriptions</span></button>
    </div>

    <div id=\\"panel-horizon\\" class=\\"card\\">
      <h2>Horizon Work Programmes</h2>
      <p class=\\"sub\\">Upload 1–6 PDFs (same run) to merge topics into a single table and Excel.</p>
      <div class=\\"stack\\">
        <div>
          <div class=\\"label\\">Call types</div>
          <select id=\\"h-types\\" multiple size=\\"6\\" class=\\"input\\">
            <option value=\\"RIA\\">RIA</option>
            <option value=\\"IA\\">IA</option>
            <option value=\\"CSA\\">CSA</option>
            <option value=\\"PCP\\">PCP</option>
            <option value=\\"PPI\\">PPI</option>
            <option value=\\"COFUND\\">COFUND</option>
            <option value=\\"ERC\\">ERC</option>
            <option value=\\"MSCA\\">MSCA</option>
            <option value=\\"EIC-PATHFINDER\\">EIC-PATHFINDER</option>
            <option value=\\"EIC-TRANSITION\\">EIC-TRANSITION</option>
            <option value=\\"EIC-ACCELERATOR\\">EIC-ACCELERATOR</option>
          </select>
          <div class=\\"hint\\">Select none = all action types.</div>
        </div>
        <div>
          <div class=\\"label\\">Min budget per project (M€)</div>
          <input class=\\"input\\" id=\\"h-budget\\" type=\\"number\\" min=\\"0\\" max=\\"15\\" step=\\"1\\" value=\\"0\\"/>
          <div class=\\"hint\\">15 = 15+ M€.</div>
        </div>
        <div>
          <div class=\\"label\\">Opening date filter</div>
          <input class=\\"input\\" id=\\"h-open\\" placeholder=\\"2026 or 2026-Q1 or 2026-05\\"/>
        </div>
        <div>
          <div class=\\"label\\">Deadline date filter</div>
          <input class=\\"input\\" id=\\"h-deadline\\" placeholder=\\"2027 or 2027-Q4 or 2027-11-15\\"/>
        </div>
      </div>

      <div class=\\"label\\" style=\\"margin-top:12px;\\">Upload up to 6 PDFs</div>
      <div class=\\"drop\\" id=\\"h-drop\\">
        <input id=\\"h-files\\" type=\\"file\\" accept=\\"application/pdf\\" multiple style=\\"position:absolute;inset:0;opacity:0;cursor:pointer;\\">
        <div class=\\"row\\" style=\\"justify-content:space-between;\\">
          <div>
            <strong>Select PDFs</strong>
            <div class=\\"hint\\">Horizon Work Programme only. Long names wrap automatically.</div>
          </div>
          <div class=\\"row\\">
            <button class=\\"btn ghost\\" id=\\"h-clear\\" disabled>Clear</button>
            <button class=\\"btn primary\\" id=\\"h-run\\" disabled>Generate Excel</button>
          </div>
        </div>
        <div class=\\"filelist\\" id=\\"h-filelist\\"></div>
      </div>
      <div class=\\"status\\" id=\\"h-status\\">Waiting for files.</div>
      <div class=\\"download\\">
        <button class=\\"btn\\" id=\\"h-download\\" style=\\"display:none;\\">Download Excel</button>
        <span class=\\"badge\\" id=\\"h-count\\"></span>
      </div>
      <div class=\\"table-wrap\\" id=\\"h-table-wrap\\" style=\\"display:none;\\">
        <table id=\\"h-table\\"></table>
      </div>
    </div>

    <div id=\\"panel-edf\\" class=\\"card\\" style=\\"margin-top:16px;display:none;\\">
      <h2>EDF – European Defence Fund</h2>
      <p class=\\"sub\\">Upload a single EDF call topic descriptions PDF. Topic links are hidden by design.</p>
      <div class=\\"stack\\">
        <div>
          <div class=\\"label\\">Call family</div>
          <select id=\\"e-families\\" multiple size=\\"6\\" class=\\"input\\">
            <option value=\\"RA\\">RA</option>
            <option value=\\"DA\\">DA</option>
            <option value=\\"LS-RA\\">LS-RA</option>
            <option value=\\"LS-DA\\">LS-DA</option>
            <option value=\\"DIS\\">DIS</option>
          </select>
          <div class=\\"hint\\">Select none = all families.</div>
        </div>
        <div>
          <div class=\\"label\\">Budget range (M€)</div>
          <div class=\\"row\\">
            <input class=\\"input\\" id=\\"e-min\\" type=\\"number\\" min=\\"0\\" step=\\"1\\" placeholder=\\"min\\" style=\\"width:120px;\\">
            <input class=\\"input\\" id=\\"e-max\\" type=\\"number\\" min=\\"0\\" step=\\"1\\" placeholder=\\"max\\" style=\\"width:120px;\\">
          </div>
        </div>
        <div>
          <div class=\\"label\\">STEP</div>
          <select id=\\"e-step\\" class=\\"input\\">
            <option value=\\"any\\">Any</option>
            <option value=\\"yes\\">STEP only</option>
          </select>
        </div>
        <div>
          <div class=\\"label\\">Title keyword</div>
          <input class=\\"input\\" id=\\"e-query\\" placeholder=\\"e.g. chemical\\">
        </div>
      </div>

      <div class=\\"label\\" style=\\"margin-top:12px;\\">Upload EDF PDF</div>
      <div class=\\"drop\\" id=\\"e-drop\\">
        <input id=\\"e-file\\" type=\\"file\\" accept=\\"application/pdf\\" style=\\"position:absolute;inset:0;opacity:0;cursor:pointer;\\">
        <div class=\\"row\\" style=\\"justify-content:space-between;\\">
          <div>
            <strong>Select PDF</strong>
            <div class=\\"hint\\">EDF call topic descriptions only.</div>
          </div>
          <div class=\\"row\\">
            <button class=\\"btn ghost\\" id=\\"e-clear\\" disabled>Clear</button>
            <button class=\\"btn primary\\" id=\\"e-run\\" disabled>Generate Excel</button>
          </div>
        </div>
        <div class=\\"filelist\\" id=\\"e-filelist\\"></div>
      </div>
      <div class=\\"status\\" id=\\"e-status\\">Waiting for file.</div>
      <div class=\\"download\\">
        <button class=\\"btn\\" id=\\"e-download\\" style=\\"display:none;\\">Download Excel</button>
        <span class=\\"badge\\" id=\\"e-count\\"></span>
      </div>
      <div class=\\"table-wrap\\" id=\\"e-table-wrap\\" style=\\"display:none;\\">
        <table id=\\"e-table\\"></table>
      </div>
    </div>
  </div>

  <script>
    const $ = (id) => document.getElementById(id);

    const state = {
      tab: "horizon",
      horizon: { files: [], rows: [], download: null, busy: false },
      edf: { file: null, rows: [], download: null, busy: false },
    };

    function setTab(tab){
      state.tab = tab;
      $("panel-horizon").style.display = tab === "horizon" ? "block" : "none";
      $("panel-edf").style.display = tab === "edf" ? "block" : "none";
      $("tab-horizon").classList.toggle("active", tab === "horizon");
      $("tab-edf").classList.toggle("active", tab === "edf");
    }

    $("tab-horizon").onclick = () => setTab("horizon");
    $("tab-edf").onclick = () => setTab("edf");

    function setStatus(target, msg, isErr=false){
      const el = $(target);
      el.textContent = msg;
      el.classList.toggle("error", isErr);
      el.classList.toggle("success", !isErr && msg.toLowerCase().includes("ready"));
    }

    function fmtBytes(b){
      if(!b && b !== 0) return "";
      const u = ["B","KB","MB","GB"];
      let i=0,n=b;
      while(n>=1024 && i<u.length-1){ n/=1024; i++; }
      return (i===0?n:n.toFixed(1))+" "+u[i];
    }

    function renderFiles(){
      const box = $("h-filelist");
      box.innerHTML = "";
      state.horizon.files.forEach((f, idx)=>{
        const row = document.createElement("div");
        row.className = "filechip";
        row.innerHTML = `<span>${f.name} (${fmtBytes(f.size)})</span>`;
        const btn = document.createElement("button");
        btn.className = "btn ghost";
        btn.textContent = "Remove";
        btn.onclick = ()=>{
          state.horizon.files.splice(idx,1);
          renderFiles();
        };
        row.appendChild(btn);
        box.appendChild(row);
      });
      $("h-clear").disabled = state.horizon.files.length===0 || state.horizon.busy;
      $("h-run").disabled = state.horizon.files.length===0 || state.horizon.busy;
      setStatus("h-status", state.horizon.files.length ? `${state.horizon.files.length} file(s) ready (Horizon only)` : "Waiting for files.");
    }

    $("h-files").addEventListener("change",(e)=>{
      const files = Array.from(e.target.files||[]).slice(0,6);
      const ok = files.filter(f=> (f.type||"").includes("pdf") || (f.name||"").toLowerCase().endsWith(".pdf"));
      state.horizon.files = ok.slice(0,6);
      renderFiles();
    });

    $("h-clear").onclick = ()=>{ state.horizon.files=[]; $("h-files").value=""; renderFiles(); };

    function renderEdfFile(){
      const box = $("e-filelist");
      box.innerHTML = "";
      if(state.edf.file){
        const row = document.createElement("div");
        row.className = "filechip";
        row.innerHTML = `<span>${state.edf.file.name} (${fmtBytes(state.edf.file.size)})</span>`;
        box.appendChild(row);
      }
      $("e-clear").disabled = !state.edf.file || state.edf.busy;
      $("e-run").disabled = !state.edf.file || state.edf.busy;
      setStatus("e-status", state.edf.file ? "File ready (EDF only)" : "Waiting for file.");
    }

    $("e-file").addEventListener("change",(e)=>{
      const f = (e.target.files||[])[0];
      if(!f) return;
      if(!( (f.type||"").includes("pdf") || (f.name||"").toLowerCase().endsWith(".pdf") )){
        setStatus("e-status","PDF required.",true);
        $("e-file").value="";
        return;
      }
      state.edf.file = f;
      renderEdfFile();
    });
    $("e-clear").onclick = ()=>{ state.edf.file=null; $("e-file").value=""; renderEdfFile(); };

    function tableHorizon(rows){
      const table = $("h-table");
      table.innerHTML = "";
      const headers = ["Topic ID","Action Type","Call/Stage","Topic Title","Min budget (M€)","Opening","Deadline"];
      const keys = ["topic_id","action_type","type","topic_title","budget_per_project_min_eur_m","opening_date","deadline_date"];
      const thead = document.createElement("thead");
      const tr = document.createElement("tr");
      headers.forEach(h=>{ const th=document.createElement("th"); th.textContent=h; tr.appendChild(th);});
      thead.appendChild(tr); table.appendChild(thead);
      const tbody = document.createElement("tbody");
      rows.forEach(r=>{
        const row = document.createElement("tr");
        keys.forEach((k,idx)=>{
          const td = document.createElement("td");
          td.setAttribute("data-label", headers[idx]);
          let val = r[k] ?? "";
          if(k==="topic_id" && val){
            const a=document.createElement("a"); a.href=r.topic_url||`https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-details/${val}`; a.target="_blank"; a.rel="noreferrer"; a.textContent=val; td.appendChild(a);
          } else {
            td.textContent = val || "—";
          }
          row.appendChild(td);
        });
        tbody.appendChild(row);
      });
      table.appendChild(tbody);
      $("h-table-wrap").style.display = rows.length ? "block" : "none";
      $("h-count").textContent = rows.length ? `${rows.length} topics` : "";
      if(rows.length){ table.scrollIntoView({behavior:"smooth",block:"start"}); }
    }

    function tableEdf(rows){
      const table = $("e-table");
      table.innerHTML = "";
      const headers = ["Topic ID","Call ID","Title","Type of action","Indicative budget (M€)","Actions to fund","STEP"];
      const keys = ["topic_id","call_id","topic_title","type_of_action","indicative_budget_eur_m","number_of_actions_to_be_funded","step_flag"];
      const thead = document.createElement("thead");
      const tr=document.createElement("tr");
      headers.forEach(h=>{ const th=document.createElement("th"); th.textContent=h; tr.appendChild(th);});
      thead.appendChild(tr); table.appendChild(thead);
      const tbody=document.createElement("tbody");
      rows.forEach(r=>{
        const row=document.createElement("tr");
        keys.forEach((k,idx)=>{
          const td=document.createElement("td");
          td.setAttribute("data-label", headers[idx]);
          td.textContent = r[k] ?? "—";
          row.appendChild(td);
        });
        tbody.appendChild(row);
      });
      table.appendChild(tbody);
      $("e-table-wrap").style.display = rows.length ? "block" : "none";
      $("e-count").textContent = rows.length ? `${rows.length} topics` : "";
      if(rows.length){ table.scrollIntoView({behavior:"smooth",block:"start"}); }
    }

    async function runHorizon(){
      if(!state.horizon.files.length){ setStatus("h-status","Select at least one PDF.",true); return; }
      if(state.horizon.files.length>6){ setStatus("h-status","Max 6 PDFs.",true); return; }
      state.horizon.busy=true; renderFiles(); setStatus("h-status","Preparing uploads…");
      try{
        const pres = await fetchJson(`/presign?n=${state.horizon.files.length}`);
        const uploads = pres.uploads || [];
        if(uploads.length !== state.horizon.files.length) throw new Error("Presign mismatch");
        for(let i=0;i<uploads.length;i++){
          const put = await fetch(uploads[i].upload_url,{method:"PUT",body:state.horizon.files[i]});
          if(!put.ok) throw new Error("Upload failed: HTTP "+put.status);
        }
        setStatus("h-status","Parsing Horizon PDFs…");
        const types = Array.from($("h-types").selectedOptions||[]).map(o=>o.value);
        const body = {
          pdf_keys: uploads.map(u=>u.pdf_key),
          expected_type:"horizon",
          action_types: types,
          min_budget_m: Number($("h-budget").value||0),
          opening_filter: $("h-open").value||"",
          deadline_filter: $("h-deadline").value||"",
          original_names: state.horizon.files.map(f=>f.name||""),
        };
        const proc = await fetchJson("/process",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
        const dl = await fetchJson("/download",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({excel_key:proc.excel_key})});
        state.horizon.download = dl.download_url;
        state.horizon.rows = proc.rows || [];
        $("h-download").style.display = "inline-flex";
        $("h-download").onclick = ()=>{ if(state.horizon.download) window.location = state.horizon.download; };
        setStatus("h-status","Ready • Horizon extraction complete.");
        tableHorizon(state.horizon.rows);
      }catch(e){
        setStatus("h-status", e.message || "Error during processing", true);
      }finally{
        state.horizon.busy=false; renderFiles();
      }
    }

    async function runEdf(){
      if(!state.edf.file){ setStatus("e-status","Select an EDF PDF.",true); return; }
      state.edf.busy=true; renderEdfFile(); setStatus("e-status","Uploading…");
      try{
        const pres = await fetchJson("/presign");
        const put = await fetch(pres.upload_url,{method:"PUT",body:state.edf.file});
        if(!put.ok) throw new Error("Upload failed: HTTP "+put.status);
        setStatus("e-status","Parsing EDF PDF…");
        const families = Array.from($("e-families").selectedOptions||[]).map(o=>o.value);
        const body = {
          pdf_key: pres.pdf_key,
          expected_type:"edf",
          call_families: families,
          min_budget_edf: $("e-min").value ? Number($("e-min").value) : null,
          max_budget_edf: $("e-max").value ? Number($("e-max").value) : null,
          step_only: $("e-step").value === "yes",
          title_query: $("e-query").value || "",
          original_name: state.edf.file?.name || "",
        };
        const proc = await fetchJson("/process",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
        const dl = await fetchJson("/download",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({excel_key:proc.excel_key})});
        state.edf.download = dl.download_url;
        state.edf.rows = proc.rows || [];
        $("e-download").style.display="inline-flex";
        $("e-download").onclick = ()=>{ if(state.edf.download) window.location = state.edf.download; };
        setStatus("e-status","Ready • EDF extraction complete.");
        tableEdf(state.edf.rows);
      }catch(e){
        setStatus("e-status", e.message || "Error during processing", true);
      }finally{
        state.edf.busy=false; renderEdfFile();
      }
    }

    $("h-run").onclick = runHorizon;
    $("e-run").onclick = runEdf;

    // Drag states
    function bindDrag(zone,input){
      ["dragenter","dragover"].forEach(evt=>zone.addEventListener(evt,(e)=>{e.preventDefault();zone.classList.add("drag");}));
      ["dragleave","drop"].forEach(evt=>zone.addEventListener(evt,(e)=>{e.preventDefault();zone.classList.remove("drag");}));
      zone.addEventListener("click",(e)=>{ if(e.target.tagName !== "BUTTON") input.click(); });
    }
    bindDrag($("h-drop"), $("h-files"));
    bindDrag($("e-drop"), $("e-file"));

    renderFiles(); renderEdfFile();
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
            key = event.get("pdf_key")
            keys = event.get("pdf_keys") or ([key] if key else [])
            action_types = event.get("action_types")
            min_budget_m = _coerce_float(event.get("min_budget_m"))
            if min_budget_m is None:
                min_budget_m = DEFAULT_MIN_BUDGET_M
            original_name = event.get("original_name") or ""
            opening_filter = event.get("opening_filter") or ""
            deadline_filter = event.get("deadline_filter") or ""
            expected_type = (event.get("expected_type") or "").strip().lower()
            call_families = event.get("call_families")
            min_budget_edf = _coerce_float(event.get("min_budget_edf"))
            max_budget_edf = _coerce_float(event.get("max_budget_edf"))
            step_only = bool(event.get("step_only"))
            title_query = event.get("title_query") or ""
            original_names = event.get("original_names") or ([original_name] if original_name else [])
            return _process_pdf_keys(
                keys,
                context=context,
                expected_type=expected_type,
                action_types=action_types,
                min_budget_m=min_budget_m,
                opening_filter=opening_filter,
                deadline_filter=deadline_filter,
                call_families=call_families,
                min_budget_edf=min_budget_edf,
                max_budget_edf=max_budget_edf,
                step_only=step_only,
                title_query=title_query,
                original_names=original_names,
            )

        method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
        path = event.get("rawPath", "/")

        if method == "OPTIONS":
            return _resp(200, "")

        if method == "GET" and path == "/":
            return _resp(200, HTML, content_type="text/html; charset=utf-8")

        if method == "GET" and path == "/presign":
            _require_bucket()
            qs = event.get("queryStringParameters") or {}
            try:
                count = int(qs.get("n", "1"))
            except Exception:
                count = 1
            count = max(1, min(count, 6))

            uploads = []
            for _ in range(count):
                pdf_key = f"uploads/{uuid.uuid4()}.pdf"
                upload_url = s3.generate_presigned_url(
                    "put_object",
                    Params={"Bucket": BUCKET, "Key": pdf_key},
                    ExpiresIn=900,
                )
                uploads.append({"upload_url": upload_url, "pdf_key": pdf_key})

            return _resp(200, _json({"uploads": uploads, "upload_url": uploads[0]["upload_url"], "pdf_key": uploads[0]["pdf_key"]}))

        if method == "POST" and path == "/process":
            _require_bucket()
            body = event.get("body") or "{}"
            data = json.loads(body)
            pdf_keys = data.get("pdf_keys") or [data.get("pdf_key")]
            action_types = data.get("action_types")
            min_budget_m = _coerce_float(data.get("min_budget_m"))
            if min_budget_m is None:
                min_budget_m = DEFAULT_MIN_BUDGET_M
            original_name = data.get("original_name") or ""
            opening_filter = data.get("opening_filter") or ""
            deadline_filter = data.get("deadline_filter") or ""
            expected_type = (data.get("expected_type") or "").strip().lower()
            call_families = data.get("call_families")
            min_budget_edf = _coerce_float(data.get("min_budget_edf"))
            max_budget_edf = _coerce_float(data.get("max_budget_edf"))
            step_only = bool(data.get("step_only"))
            title_query = data.get("title_query") or ""
            original_names = data.get("original_names") or ([original_name] if original_name else [])
            result = _process_pdf_keys(
                [k for k in pdf_keys if k],
                context=context,
                expected_type=expected_type,
                action_types=action_types,
                min_budget_m=min_budget_m,
                opening_filter=opening_filter,
                deadline_filter=deadline_filter,
                call_families=call_families,
                min_budget_edf=min_budget_edf,
                max_budget_edf=max_budget_edf,
                step_only=step_only,
                title_query=title_query,
                original_names=original_names,
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


def _process_pdf_keys(
    keys: list,
    context=None,
    expected_type: str = "",
    action_types=None,
    min_budget_m=None,
    opening_filter: str = "",
    deadline_filter: str = "",
    call_families=None,
    min_budget_edf: float = None,
    max_budget_edf: float = None,
    step_only: bool = False,
    title_query: str = "",
    original_names: Optional[list] = None,
):
    _require_bucket()
    if min_budget_m is None:
        min_budget_m = DEFAULT_MIN_BUDGET_M

    if not keys:
        raise ValueError("No pdf_key provided")

    doc_type = ""
    merged_rows = []
    used_original = original_names or []

    for idx, key in enumerate(keys):
        local_pdf = f"/tmp/{uuid.uuid4()}.pdf"
        s3.download_file(BUCKET, key, local_pdf)
        text = extract_text(local_pdf)

        detected = _detect_document_type(text)
        if not detected:
            raise ValueError("Unsupported or unrecognized document type. Please upload a Horizon WP or an EDF call topic descriptions PDF.")

        if expected_type and detected != expected_type:
            raise ValueError(f"Document type mismatch: expected {expected_type.upper()} but detected {detected.upper()}.")

        if not doc_type:
            doc_type = detected
        elif doc_type != detected:
            raise ValueError("Mixed document types are not supported in one run.")

        if detected == "horizon":
            rows = parse_calls(text)
            merged_rows.extend(rows)
        else:
            rows = parse_edf_calls(text)
            merged_rows.extend(rows)

    if doc_type == "horizon":
        for r in merged_rows:
            derived_budget = _compute_budget_per_project_m(r)
            r["budget_per_project_min_eur_m"] = derived_budget
            r["budget_per_project_m"] = derived_budget

        merged_rows = filter_rows(
            merged_rows,
            action_types=action_types,
            min_budget_m=min_budget_m,
            opening_filter=opening_filter,
            deadline_filter=deadline_filter,
        )

        # --- OpenAI descriptions (optional) ---
        desc_cache: dict = {}
        if OPENAI_API_KEY:
            n = 0
            for r in merged_rows:
                if n >= OPENAI_MAX_TOPICS:
                    break

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

        for r in merged_rows:
            if r.get("topic_description") is None:
                r["topic_description"] = ""

            if r.get("budget_per_project_min_eur_m") is None:
                derived_budget = _compute_budget_per_project_m(r)
                r["budget_per_project_min_eur_m"] = derived_budget
                r["budget_per_project_m"] = derived_budget

        for r in merged_rows:
            r.pop("topic_body", None)

        local_xlsx = f"/tmp/{uuid.uuid4()}.xlsx"
        write_horizon_xlsx(merged_rows, local_xlsx)

        safe_base = _safe_base_name("-".join([n for n in used_original if n])) or "file"
        out_key = f"outputs/{uuid.uuid4()}/{safe_base or 'horizon'}.xlsx"
        s3.upload_file(local_xlsx, BUCKET, out_key)

        recap_rows = []
        for r in merged_rows:
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

        return {"status": "ok", "excel_key": out_key, "rows": recap_rows, "rows_count": len(merged_rows), "doc_type": doc_type}

    # EDF
    merged_rows = filter_edf_rows(
        merged_rows,
        call_families=call_families,
        min_budget_m=min_budget_edf,
        max_budget_m=max_budget_edf,
        step_only=step_only,
        title_query=title_query,
    )

    local_xlsx = f"/tmp/{uuid.uuid4()}.xlsx"
    write_edf_xlsx(merged_rows, local_xlsx)
    safe_base = _safe_base_name("-".join([n for n in used_original if n])) or "edf"
    out_key = f"outputs/{uuid.uuid4()}/{safe_base or 'edf'}.xlsx"
    s3.upload_file(local_xlsx, BUCKET, out_key)

    recap_rows = []
    for r in merged_rows:
        recap_rows.append(
            {
                "topic_id": r.get("topic_id"),
                "topic_title": r.get("topic_title") or "",
                "call_id": r.get("call_id"),
                "type_of_action": r.get("type_of_action"),
                "indicative_budget_eur_m": r.get("indicative_budget_eur_m"),
                "number_of_actions_to_be_funded": r.get("number_of_actions_to_be_funded"),
                "step_flag": r.get("step_flag"),
            }
        )

    return {"status": "ok", "excel_key": out_key, "rows": recap_rows, "rows_count": len(merged_rows), "doc_type": doc_type}

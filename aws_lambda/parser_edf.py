import re
from typing import Dict, List, Optional

CALL_FAMILY_LABELS = {
    "RA": "RA — Research Actions",
    "DA": "DA — Development Actions",
    "CSA": "CSA — Coordination & Support Actions",
}

RE_TOPIC_LINE = re.compile(
    r"^\s*(?:(\d+(?:\.\d+)*)(?:\.)?)?\s*(EDF-\d{4}-[A-Z]{2,}(?:-[A-Z0-9]+)+)\s*:\s*(.+?)\s*$",
    flags=re.IGNORECASE,
)

RE_TOPIC_ONLY = re.compile(
    r"^\s*(?:(\d+(?:\.\d+)*)(?:\.)?)?\s*(EDF-\d{4}-[A-Z]{2,}(?:-[A-Z0-9]+)+)\s*$",
    flags=re.IGNORECASE,
)
RE_CALL_LINE = re.compile(
    r"^\s*(?:(\d+(?:\.\d+)*)(?:\.)?)?\s*Call\s+(EDF-\d{4}-[A-Z]{2,})\b[:\-]?\s*(.*)$",
    flags=re.IGNORECASE,
)
RE_CALL = re.compile(r"\b(EDF-\d{4}-[A-Z]{2,})\b", flags=re.IGNORECASE)
TOC_START = re.compile(r"\bTable of contents\b", re.IGNORECASE)
TOC_END = re.compile(r"^\s*1\.\s*Content of the document\b", re.IGNORECASE)

_STOPWORDS = {"of", "in", "on", "to", "by", "an", "or", "if", "at", "be", "is", "it", "we", "il", "la", "le", "un", "una"}
BAD_TITLE_HINTS = [
    "SENSITIVE UNTIL ADOPTION",
    "Content of the document",
    "Table of contents",
    "Appendix",
    "<<<PAGE",
]


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\u00ad", "").strip())


def _extract_call_family(call_id: Optional[str]) -> Optional[str]:
    if not call_id:
        return None
    parts = call_id.split("-")
    fam = parts[2] if len(parts) >= 3 else None
    if not fam:
        return None
    fam = fam.upper()
    return fam if fam in CALL_FAMILY_LABELS else None


def _has_large_scale_token(identifier: Optional[str]) -> bool:
    if not identifier:
        return False
    parts = identifier.split("-")
    if len(parts) < 4:
        return False
    return any(p.upper() == "LS" for p in parts[3:])


def _is_large_scale(call_id: Optional[str], topic_id: Optional[str], title: str, desc: str) -> bool:
    if _has_large_scale_token(topic_id) or _has_large_scale_token(call_id):
        return True
    blob = " ".join([title or "", desc or ""])
    return bool(re.search(r"\blarge[-\s]?scale\b", blob, flags=re.IGNORECASE))


def _to_millions(amount_text: str) -> Optional[float]:
    if not amount_text:
        return None
    cleaned = re.sub(r"[^\d,.\s]", "", amount_text)
    cleaned = cleaned.replace(" ", "").replace(",", "")
    if not cleaned:
        return None
    try:
        value = float(cleaned)
    except ValueError:
        return None
    return round(value / 1_000_000, 2)


def _extract_budget(line: str) -> Optional[float]:
    m = re.search(r"EUR\s*([0-9][0-9 .,\u00a0]*)", line, flags=re.IGNORECASE)
    if not m:
        m = re.search(r"([0-9][0-9 .,\u00a0]*)\s*EUR", line, flags=re.IGNORECASE)
    if not m:
        return None
    return _to_millions(m.group(1))


def _repair_broken_words(title: str) -> str:
    """
    Fix split words caused by PDF line breaks without hyphens, e.g. "mo dels" -> "models".
    We only merge when the first fragment is very short and not a common standalone word to avoid false positives.
    """
    def repl(match: re.Match) -> str:
        first = match.group(1)
        second = match.group(2)
        if len(first) <= 2 and first.lower() not in _STOPWORDS and first.isalpha() and first.islower() and second.isalpha() and second.islower():
            return first + second
        return match.group(0)

    return re.sub(r"\b([A-Za-z]{1,3})\s+([a-z]{3,})\b", repl, title)


def _clean_title(title: str) -> str:
    cleaned = _norm(title)
    if not cleaned:
        return ""
    cleaned = re.sub(r"\.{2,}\s*\d*\s*$", "", cleaned)  # strip dotted leaders and trailing page numbers
    cleaned = cleaned.strip(" .-–")
    cleaned = _repair_broken_words(cleaned)
    return cleaned


def _is_bad_title(t: str) -> bool:
    if not t:
        return True
    up = t.upper()
    if any(h.upper() in up for h in BAD_TITLE_HINTS):
        return True
    if len(t) > 140:
        return True
    return False


def _looks_like_title_fragment(line: str) -> bool:
    low = line.lower()
    if not line or len(line.split()) < 2:
        return False
    if "type of action" in low or "indicative budget" in low or "number of actions" in low or low.strip() == "step":
        return False
    if RE_TOPIC_ONLY.search(line) or RE_CALL.search(line):
        return False
    return True


def _extract_topic_budget_eur_m(line: str) -> Optional[float]:
    if "indicative budget" in line.lower() and "for this topic" in line.lower():
        return _extract_budget(line)
    return None


def _extract_call_budget_eur_m(line: str) -> Optional[float]:
    if "indicative budget for the call" in line.lower():
        return _extract_budget(line)
    return None


def _extract_funding_percentage(line: str) -> Optional[float]:
    """
    Return numeric percentage only when the line explicitly mentions funding-related wording.
    Avoids guessing unrelated percentages.
    """
    low = line.lower()
    if not any(
        kw in low
        for kw in [
            "funding rate",
            "funding level",
            "funding intensity",
            "funding percentage",
            "eu funding",
            "union funding",
            "co-funding",
            "cofunding",
        ]
    ):
        return None

    m = re.search(r"(\d{1,3})\s?%", line)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def parse_edf(text: str) -> List[Dict]:
    """
    Extract EDF calls and topics with a lightweight heuristic parser.
    Fields include record_level (CALL|TOPIC), call_id, topic_id, title, section_no,
    type_of_action, indicative budgets, call_family, step, and verbatim description.
    """
    raw_lines = (text or "").splitlines()

    records: List[Dict] = []
    current: Optional[Dict] = None
    current_call_id: Optional[str] = None
    current_call_section: Optional[str] = None
    current_call_record: Optional[Dict] = None
    in_toc = False

    def _call_family_topic(tid: str) -> Optional[str]:
        m = RE_CALL.search(tid or "")
        if m:
            return m.group(1).upper()
        parts = (tid or "").split("-")
        if len(parts) >= 3:
            return "-".join(parts[:3]).upper()
        return None

    def ensure_current(topic_id: str, title: str = "", awaiting_title: bool = False, section_no: Optional[str] = None):
        nonlocal current
        if current:
            current["_in_desc"] = False
        cleaned_title = _clean_title(title)
        if _is_bad_title(cleaned_title):
            cleaned_title = ""
        call_id = current_call_id or _call_family_topic(topic_id)
        current = {
            "record_level": "TOPIC",
            "call_id": call_id,
            "topic_id": topic_id,
            "topic_title": cleaned_title,
            "title": cleaned_title or topic_id,
            "section_no": section_no,
            "type_of_action": "",
            "indicative_budget_eur_m": None,
            "call_indicative_budget_eur_m": None,
            "number_of_actions": None,
            "call_family": _extract_call_family(call_id),
            "step": None,
            "page": None,
            "topic_description_verbatim": "",
            "is_large_scale": False,
            "funding_percentage": None,
            "opening_date": None,
            "deadline_date": None,
            "_in_desc": False,
            "_awaiting_title": awaiting_title or not cleaned_title,
        }
        records.append(current)

    for raw_ln in raw_lines:
        ln = _norm(raw_ln)

        if TOC_START.search(ln):
            in_toc = True
            continue
        if in_toc and TOC_END.search(ln):
            in_toc = False
            continue
        if in_toc:
            continue

        # Call header (section + call id + optional title)
        m_call_line = RE_CALL_LINE.match(ln)
        if m_call_line:
            section_no = m_call_line.group(1)
            call_id = m_call_line.group(2).upper()
            call_title = _clean_title(m_call_line.group(3)) or call_id

            current_call_id = call_id
            current_call_section = section_no
            current_call_record = {
                "record_level": "CALL",
                "call_id": call_id,
                "topic_id": None,
                "topic_title": "",
                "title": call_title,
                "section_no": section_no,
                "type_of_action": "",
                "indicative_budget_eur_m": None,
                "call_indicative_budget_eur_m": None,
                "number_of_actions": None,
                "call_family": _extract_call_family(call_id),
                "step": None,
                "page": None,
                "topic_description_verbatim": "",
                "is_large_scale": _is_large_scale(call_id, None, call_title, ""),
                "funding_percentage": None,
                "opening_date": None,
                "deadline_date": None,
            }
            records.append(current_call_record)
            continue

        # Topic header with inline title
        m_line = RE_TOPIC_LINE.match(ln)
        if m_line:
            section_no = m_line.group(1)
            topic_id = m_line.group(2).upper()
            title = m_line.group(3)
            ensure_current(topic_id, title, section_no=section_no)
            continue

        # Topic header without inline title
        m_topic = RE_TOPIC_ONLY.search(ln)
        if m_topic:
            section_no = m_topic.group(1)
            topic_id = m_topic.group(2 if m_topic.lastindex and m_topic.lastindex >= 2 else 1).upper()
            ensure_current(topic_id, "", awaiting_title=True, section_no=section_no)
            continue

        if current is None:
            continue

        # Title continuation (short line immediately after the ID)
        if current.get("_awaiting_title") and _looks_like_title_fragment(ln):
            fragment = _clean_title(ln)
            if fragment and not _is_bad_title(fragment):
                current["topic_title"] = fragment
                current["_awaiting_title"] = False

        # Type of action
        if "type of action" in ln.lower():
            tail = ln.split(":", 1)[1].strip() if ":" in ln else ln
            current["type_of_action"] = tail or current.get("type_of_action") or ""

        # Budget separation (topic vs call)
        call_budget = _extract_call_budget_eur_m(ln)
        if call_budget is not None:
            if current_call_record is not None:
                current_call_record["call_indicative_budget_eur_m"] = call_budget
            if current is not None:
                current["call_indicative_budget_eur_m"] = call_budget

        topic_budget = _extract_topic_budget_eur_m(ln)
        if topic_budget is not None:
            current["indicative_budget_eur_m"] = topic_budget

        funding_pct = _extract_funding_percentage(ln)
        if funding_pct is not None and current.get("funding_percentage") is None:
            current["funding_percentage"] = funding_pct

        # Number of actions
        if "number of actions" in ln.lower():
            m_num = re.search(r"(\d+)", ln)
            if m_num:
                current["number_of_actions"] = int(m_num.group(1))

        # STEP flag
        if "step" in ln.lower():
            if re.search(r"\bstep\b.*\byes\b", ln, flags=re.IGNORECASE):
                current["step"] = True
            elif re.search(r"\bstep\b.*\bno\b", ln, flags=re.IGNORECASE):
                current["step"] = False
            elif current.get("step") is None and "step" in ln.upper():
                current["step"] = True

        # Topic description verbatim
        low = ln.lower()
        start_desc = any(
            h in low
            for h in [
                "objectives",
                "general objective",
                "specific objective",
                "scope and types of activities",
            ]
        )

        if start_desc:
            current["_in_desc"] = True

        if current.get("_in_desc"):
            if current["topic_description_verbatim"]:
                current["topic_description_verbatim"] += "\n"
            current["topic_description_verbatim"] += raw_ln.rstrip()

        low = ln.lower()
        if "opening date" in low:
            tail = ln.split(":", 1)[1].strip() if ":" in ln else ln
            current["opening_date"] = tail or current.get("opening_date")
        if "deadline" in low:
            tail = ln.split(":", 1)[1].strip() if ":" in ln else ln
            current["deadline_date"] = tail or current.get("deadline_date")

        # Topic title fallback (avoid prefatory garbage)
        if (
            current.get("_awaiting_title")
            and not current.get("topic_title")
            and len(ln.split()) > 3
            and not RE_CALL.search(ln)
        ):
            candidate = _clean_title(ln)
            if candidate and not _is_bad_title(candidate):
                current["topic_title"] = candidate
                current["_awaiting_title"] = False

    for t in records:
        t.pop("_in_desc", None)
        t.pop("_awaiting_title", None)
        call_id = t.get("call_id")
        topic_id = t.get("topic_id")
        t["call_family"] = t.get("call_family") or _extract_call_family(call_id) or _extract_call_family(topic_id)
        t["record_level"] = t.get("record_level") or "TOPIC"
        if "title" not in t:
            t["title"] = t.get("topic_title") or t.get("call_id") or t.get("topic_id")
        t["is_large_scale"] = _is_large_scale(call_id, topic_id, t.get("title", ""), t.get("topic_description_verbatim", ""))

    return records

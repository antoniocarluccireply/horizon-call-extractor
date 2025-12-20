import re
from typing import Dict, List, Optional

RE_TOPIC_LINE = re.compile(
    r"^\s*(?:\d+(?:\.\d+)*\.)?\s*(EDF-\d{4}-[A-Z]{2,}(?:-[A-Z0-9]+)+)\s*:\s*(.+?)\s*$",
    flags=re.IGNORECASE,
)

RE_TOPIC_ONLY = re.compile(r"\b(EDF-\d{4}-[A-Z]{2,}(?:-[A-Z0-9]+)+)\b", flags=re.IGNORECASE)
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


def _call_family(call_id: Optional[str]) -> Optional[str]:
    if not call_id:
        return None
    parts = call_id.split("-")
    return parts[2] if len(parts) >= 3 else None


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


def parse_edf(text: str) -> List[Dict]:
    """
    Extract EDF topics with a lightweight heuristic parser.
    Fields: call_id, topic_id, topic_title, type_of_action, indicative_budget_eur_m,
    call_indicative_budget_eur_m, number_of_actions, call_family, step, topic_description_verbatim.
    """
    raw_lines = (text or "").splitlines()

    topics: List[Dict] = []
    current: Optional[Dict] = None
    in_toc = False

    def _call_family_topic(tid: str) -> Optional[str]:
        m = RE_CALL.search(tid or "")
        if m:
            return m.group(1).upper()
        parts = (tid or "").split("-")
        if len(parts) >= 3:
            return "-".join(parts[:3]).upper()
        return None

    def ensure_current(topic_id: str, title: str = "", awaiting_title: bool = False):
        nonlocal current
        if current:
            current["_in_desc"] = False
        cleaned_title = _clean_title(title)
        if _is_bad_title(cleaned_title):
            cleaned_title = ""
        current = {
            "call_id": _call_family_topic(topic_id),
            "topic_id": topic_id,
            "topic_title": cleaned_title,
            "type_of_action": "",
            "indicative_budget_eur_m": None,
            "call_indicative_budget_eur_m": None,
            "number_of_actions": None,
            "call_family": _call_family(_call_family_topic(topic_id)),
            "step": None,
            "page": None,
            "topic_description_verbatim": "",
            "_in_desc": False,
            "_awaiting_title": awaiting_title or not cleaned_title,
        }
        topics.append(current)

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

        # Topic header with inline title
        m_line = RE_TOPIC_LINE.match(ln)
        if m_line:
            topic_id = m_line.group(1).upper()
            title = m_line.group(2)
            ensure_current(topic_id, title)
            continue

        # Topic header without inline title
        m_topic = RE_TOPIC_ONLY.search(ln)
        if m_topic:
            topic_id = m_topic.group(1).upper()
            ensure_current(topic_id, "", awaiting_title=True)
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
            current["call_indicative_budget_eur_m"] = call_budget

        topic_budget = _extract_topic_budget_eur_m(ln)
        if topic_budget is not None:
            current["indicative_budget_eur_m"] = topic_budget

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

    for t in topics:
        t.pop("_in_desc", None)
        t.pop("_awaiting_title", None)

    return topics

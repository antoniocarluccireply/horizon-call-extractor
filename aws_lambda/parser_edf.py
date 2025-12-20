import re
from typing import Dict, List, Optional

RE_TOPIC = re.compile(r"\b(EDF-\d{4}-[A-Z]{2,}(?:-[A-Z0-9]+)+)\b", flags=re.IGNORECASE)
RE_CALL = re.compile(r"\b(EDF-\d{4}-[A-Z]{2,})\b", flags=re.IGNORECASE)


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


def parse_edf(text: str) -> List[Dict]:
    """
    Extract EDF topics with a lightweight heuristic parser.
    Fields: call_id, topic_id, topic_title, type_of_action, indicative_budget_eur_m,
    number_of_actions, call_family, step.
    """
    lines = [_norm(ln) for ln in (text or "").splitlines()]
    lines = [ln for ln in lines if ln]

    topics: List[Dict] = []
    current: Optional[Dict] = None

    def ensure_current(topic_id: str, title: str = ""):
        nonlocal current
        current = {
            "call_id": _call_family_topic(topic_id),
            "topic_id": topic_id,
            "topic_title": title,
            "type_of_action": "",
            "indicative_budget_eur_m": None,
            "number_of_actions": None,
            "call_family": _call_family(_call_family_topic(topic_id)),
            "step": None,
            "page": None,
        }
        topics.append(current)

    def _call_family_topic(tid: str) -> Optional[str]:
        m = RE_CALL.search(tid or "")
        if m:
            return m.group(1).upper()
        parts = (tid or "").split("-")
        if len(parts) >= 3:
            return "-".join(parts[:3]).upper()
        return None

    for ln in lines:
        # Topic header
        m_topic = RE_TOPIC.search(ln)
        if m_topic:
            topic_id = m_topic.group(1).upper()
            title = ""
            if ":" in ln:
                title = ln.split(":", 1)[1].strip(" -–")
            elif ln.rstrip().upper() != topic_id:
                title = ln.replace(topic_id, "").strip(" :-–")
            ensure_current(topic_id, title)
            continue

        if current is None:
            continue

        # Type of action
        if "type of action" in ln.lower():
            tail = ln.split(":", 1)[1].strip() if ":" in ln else ln
            current["type_of_action"] = tail or current.get("type_of_action") or ""

        # Budget
        if "indicative budget" in ln.lower() or "budget" in ln.lower():
            budget = _extract_budget(ln)
            if budget is not None:
                current["indicative_budget_eur_m"] = budget

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

        # Topic title fallback
        if not current.get("topic_title") and len(ln.split()) > 3 and not RE_CALL.search(ln):
            current["topic_title"] = ln

    return topics

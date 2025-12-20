import re
from typing import Dict, List, Optional, Tuple


RE_CALL_ID = re.compile(r"\bEDF-20\d{2}-[A-Z]{2,}(?:-[A-Z]{2,})?\b")
RE_TOPIC_ID = re.compile(r"\bEDF-20\d{2}-[A-Z0-9]+(?:-[A-Z0-9]+)+\b")

RE_PAGE_MARKER = re.compile(r"^<<<PAGE\s+(\d+)>>>$")


def _norm(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("\u00ad", "")
    s = s.replace("\u2010", "-")
    s = s.replace("\u2011", "-")
    s = s.replace("\u2013", "-")
    s = s.replace("\u2014", "-")
    s = s.replace("\u2212", "-")
    s = s.replace("\u2019", "'")
    return re.sub(r"\s+", " ", s).strip()


def _extract_budget(lines: List[str], start_i: int, max_lines: int = 24) -> Tuple[Optional[float], Optional[float], int]:
    budget_m = None
    actions = None
    i = start_i

    for _ in range(max_lines):
        if i >= len(lines):
            break

        ln = lines[i].lower()

        m_range = re.search(r"indicative budget[^\d]*(\d{1,4}(?:\.\d{1,2})?)\s*(?:million|m?)", ln)
        if m_range:
            try:
                budget_m = float(m_range.group(1))
            except Exception:
                budget_m = None

        m_actions = re.search(r"number of actions[^\d]*(\d{1,2})", ln)
        if m_actions:
            try:
                actions = int(m_actions.group(1))
            except Exception:
                actions = None

        if budget_m is not None and actions is not None:
            return budget_m, actions, i + 1

        # Detect STEP keyword near the budget section
        i += 1

    return budget_m, actions, i


def _detect_action(lines: List[str], start_i: int, max_lines: int = 16) -> Tuple[Optional[str], int]:
    i = start_i
    for _ in range(max_lines):
        if i >= len(lines):
            break

        ln = lines[i].lower()
        if "research action" in ln:
            return "Research actions", i + 1
        if "development action" in ln:
            return "Development actions", i + 1
        if "type of action" in ln:
            # e.g. "Type of action: Research actions"
            m = re.search(r"type of action[:\-]?\s*(.+)$", ln)
            if m:
                return m.group(1).strip().capitalize(), i + 1

        i += 1

    return None, i


def _extract_title(lines: List[str], start_i: int) -> Tuple[str, int]:
    parts: List[str] = []
    i = start_i
    for _ in range(6):
        if i >= len(lines):
            break

        ln = _norm(lines[i])
        if not ln:
            i += 1
            continue

        if RE_TOPIC_ID.search(ln):
            break
        if ln.lower().startswith("topic" ):
            ln = ln.split(":", 1)[-1].strip()

        if ln:
            parts.append(ln)

        # stop if we collected a sentence-like line
        if len(" ".join(parts)) > 160:
            break
        i += 1

    return _norm(" ".join(parts)), i


def parse_edf_calls(text: str) -> List[Dict]:
    raw_lines = [_norm(ln) for ln in (text or "").splitlines() if _norm(ln)]

    current_page: Optional[int] = None
    current_call_id: Optional[str] = None

    rows: List[Dict] = []

    i = 0
    while i < len(raw_lines):
        ln = raw_lines[i]

        m_pg = RE_PAGE_MARKER.match(ln)
        if m_pg:
            current_page = int(m_pg.group(1))
            i += 1
            continue

        m_call = RE_CALL_ID.search(ln)
        if m_call:
            current_call_id = m_call.group(0)
            i += 1
            continue

        m_topic = RE_TOPIC_ID.search(ln)
        if m_topic:
            topic_id = m_topic.group(0)
            next_i = i + 1
            title, next_i = _extract_title(raw_lines, next_i)

            type_of_action, next_i = _detect_action(raw_lines, next_i, max_lines=14)
            budget_m, actions, next_i = _extract_budget(raw_lines, next_i, max_lines=18)

            step_flag = "STEP" if "step" in ln.lower() or "step" in " ".join(raw_lines[i:next_i]).lower() else ""

            rows.append({
                "call_id": current_call_id,
                "topic_id": topic_id,
                "topic_title": title,
                "type_of_action": type_of_action,
                "indicative_budget_eur_m": budget_m,
                "number_of_actions_to_be_funded": actions,
                "step_flag": step_flag,
                "page": current_page,
            })

            i = next_i
            continue

        i += 1

    return rows

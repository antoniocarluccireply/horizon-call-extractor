# parser_horizon.py
import re
from typing import Dict, List, Optional, Tuple

# -----------------------------
# Constants / regex
# -----------------------------
ACTION_TYPES = {"RIA", "IA", "CSA", "PCP", "PPI", "COFUND"}

RE_CALL_ID = re.compile(r"\bHORIZON-[A-Z0-9]+-\d{4}-\d{2}(?:-two-stage)?\b")
RE_TOPIC_ID = re.compile(r"\bHORIZON-[A-Z0-9]+-\d{4}-\d{2}-[A-Z0-9]+(?:-[A-Z0-9]+)*(?:-two-stage)?\b")

RE_OPENING = re.compile(r"Opening:\s*(.+)")
RE_DEADLINE = re.compile(r"Deadline\(s\):\s*(.+)")

# Injected by extract_text():  <<<PAGE 14>>>
RE_PAGE_MARKER = re.compile(r"^<<<PAGE\s+(\d+)>>>$")

# TOC leaders: "............. 24"
RE_DOT_LEADER_PAGE = re.compile(r"\s\.{3,}\s*(\d{1,4})\s*$")


# -----------------------------
# Helpers
# -----------------------------
def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _strip_dot_leader_page(s: str) -> Tuple[str, Optional[int]]:
    """
    Removes TOC dot leaders like: 'Some title .................. 29'
    Returns (clean_text, page|None)
    """
    if not s:
        return s, None
    s = _norm(s)
    m = RE_DOT_LEADER_PAGE.search(s)
    if not m:
        return s, None
    page = int(m.group(1))
    cleaned = RE_DOT_LEADER_PAGE.sub("", s).strip()
    return cleaned, page


def _parse_cluster_line(ln: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
    """
    Input example:
      "Call - Cluster 1 - Health (Single stage - 2027/2) .................. 24"
    Output:
      cluster="Cluster 1 - Health"
      stage="single"
      call_round="2027/2"
      page=24   (if present via dot leaders)
    """
    raw = ln.replace("Call - ", "").strip()
    cleaned, page = _strip_dot_leader_page(raw)

    cluster = cleaned
    stage = None
    call_round = None

    if "(" in cleaned and ")" in cleaned:
        before = cleaned.split("(", 1)[0].strip()
        inside = cleaned.split("(", 1)[1].rsplit(")", 1)[0].strip()
        cluster = before

        low = inside.lower()
        if "single stage" in low:
            stage = "single"
        elif "two-stage" in low or "two stage" in low:
            stage = "two-stage"

        # usually last segment after "-"
        parts = [p.strip() for p in inside.split("-") if p.strip()]
        if len(parts) >= 2:
            call_round = parts[-1].strip()

    return cluster or None, stage, call_round, page


def _derive_call_id_from_topic(topic_id: str) -> Optional[str]:
    # topic: HORIZON-HLTH-2026-01-STAYHLTH-02  -> call: HORIZON-HLTH-2026-01
    m = re.match(r"^(HORIZON-[A-Z0-9]+-\d{4}-\d{2})-", topic_id)
    return m.group(1) if m else None


def _parse_overview_block(lines: List[str], start_i: int) -> Tuple[Optional[Dict], int]:
    """
    Parse the overview row for a topic, even when split across multiple lines by pypdf.

    We start at a line that begins with an action type (RIA/CSA/IA/...)
    and consume up to ~8 lines, stopping if a new topic/call block begins.

    Expected info in WP:
      - total budget (EUR million): e.g. 20.60
      - expected EU contribution per project (EUR million): e.g. 9.00 to 10.00 OR "Around 3.00"
      - indicative number of projects: e.g. 2

    Returns:
      ({action_type, budget_eur_m, budget_per_project_min_eur_m, budget_per_project_max_eur_m, projects}, next_index)
    """
    i = start_i
    buf: List[str] = []

    for _ in range(8):
        if i >= len(lines):
            break
        ln = lines[i].strip()
        if not ln:
            i += 1
            continue

        # stop on new blocks
        if RE_TOPIC_ID.search(ln) or ln.startswith("Call - "):
            break
        if RE_CALL_ID.search(ln) and not RE_TOPIC_ID.search(ln):
            break

        buf.append(ln)
        joined = _norm(" ".join(buf))

        # Must begin with ACTION
        m = re.match(r"^(RIA|IA|CSA|PCP|PPI|COFUND)\s+(.*)$", joined)
        if not m:
            i += 1
            continue

        action = m.group(1)
        rest = m.group(2)

        # total budget: first float in rest
        nums = re.findall(r"\d{1,4}(?:\.\d{1,2})?", rest)
        if not nums:
            i += 1
            continue
        total = float(nums[0])

        # projects: trailing integer
        proj_m = re.search(r"\b(\d{1,3})\s*$", rest)
        if not proj_m:
            i += 1
            continue
        projects = int(proj_m.group(1))

        # per-project contribution:
        range_m = re.search(r"\b(\d{1,4}(?:\.\d{1,2})?)\s+to\s+(\d{1,4}(?:\.\d{1,2})?)\b", rest)
        around_m = re.search(r"\bAround\s+(\d{1,4}(?:\.\d{1,2})?)\b", rest, flags=re.IGNORECASE)

        if range_m:
            pmin = float(range_m.group(1))
            pmax = float(range_m.group(2))
            return ({
                "action_type": action,
                "budget_eur_m": total,
                "budget_per_project_min_eur_m": pmin,
                "budget_per_project_max_eur_m": pmax,
                "projects": projects,
            }, i + 1)

        if around_m:
            p = float(around_m.group(1))
            return ({
                "action_type": action,
                "budget_eur_m": total,
                "budget_per_project_min_eur_m": p,
                "budget_per_project_max_eur_m": p,
                "projects": projects,
            }, i + 1)

        i += 1

    return None, start_i


# -----------------------------
# Main parser
# -----------------------------
def parse_calls(text: str) -> List[Dict]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    out: List[Dict] = []

    current_page: Optional[int] = None

    current_cluster: Optional[str] = None
    current_stage: Optional[str] = None
    current_call_round: Optional[str] = None
    current_cluster_page: Optional[int] = None

    current_call_id: Optional[str] = None
    current_opening: Optional[str] = None
    current_deadline: Optional[str] = None

    pending_topic_id: Optional[str] = None
    pending_title_parts: List[str] = []
    pending_page: Optional[int] = None

    # overview fields pending (because sometimes overview comes after multiple title lines)
    pending_action_type: Optional[str] = None
    pending_budget_total: Optional[float] = None
    pending_per_min: Optional[float] = None
    pending_per_max: Optional[float] = None
    pending_projects: Optional[int] = None

    def flush_topic():
        nonlocal pending_topic_id, pending_title_parts, pending_page
        nonlocal pending_action_type, pending_budget_total, pending_per_min, pending_per_max, pending_projects
        nonlocal current_call_id

        if not pending_topic_id:
            return

        title_raw = _norm(" ".join(pending_title_parts))
        title_clean, title_page = _strip_dot_leader_page(title_raw)

        # Prefer real page marker, but accept dot-leader page if present in title
        page = title_page or pending_page or current_page or current_cluster_page

        # ensure call_id always present
        if not current_call_id:
            current_call_id = _derive_call_id_from_topic(pending_topic_id)

        out.append({
            "cluster": current_cluster,
            "stage": current_stage,
            "call_round": current_call_round,
            "page": page,

            "call_id": current_call_id,
            "topic_id": pending_topic_id,
            "topic_title": title_clean,

            "action_type": pending_action_type,
            "opening_date": current_opening,
            "deadline_date": current_deadline,

            "budget_eur_m": pending_budget_total,
            "projects": pending_projects,
            "budget_per_project_min_eur_m": pending_per_min,
            "budget_per_project_max_eur_m": pending_per_max,

            # TRL is NOT in the overview row; do not invent
            "trl": None,
        })

        pending_topic_id = None
        pending_title_parts = []
        pending_page = None

        pending_action_type = None
        pending_budget_total = None
        pending_per_min = None
        pending_per_max = None
        pending_projects = None

    i = 0
    while i < len(lines):
        ln = lines[i]

        # Page marker
        m_pg = RE_PAGE_MARKER.match(ln)
        if m_pg:
            current_page = int(m_pg.group(1))
            i += 1
            continue

        # Cluster line
        if ln.startswith("Call - "):
            # Flush pending topic if any (best effort)
            flush_topic()

            cluster, stage, call_round, page = _parse_cluster_line(ln)
            current_cluster = cluster
            current_stage = stage
            current_call_round = call_round
            current_cluster_page = page

            # reset call context
            current_call_id = None
            current_opening = None
            current_deadline = None

            i += 1
            continue

        # Call id line
        m_call = RE_CALL_ID.search(ln)
        if m_call and not RE_TOPIC_ID.search(ln):
            current_call_id = m_call.group(0)
            i += 1
            continue

        # Opening / Deadline
        m_op = RE_OPENING.search(ln)
        if m_op:
            current_opening = _norm(m_op.group(1))
            i += 1
            continue

        m_dl = RE_DEADLINE.search(ln)
        if m_dl:
            current_deadline = _norm(m_dl.group(1))
            i += 1
            continue

        # Topic start
        m_topic = RE_TOPIC_ID.search(ln)
        if m_topic:
            # flush previous topic first
            flush_topic()

            pending_topic_id = m_topic.group(0)
            pending_page = current_page

            # remove dot leaders on same line if present
            cleaned_line, pg = _strip_dot_leader_page(ln)
            if pg:
                # if TOC gave a page number, store it, but prefer current_page marker later
                pending_page = pending_page or pg

            # title after ":"
            after = cleaned_line.split(":", 1)
            pending_title_parts = [_norm(after[1])] if len(after) == 2 else []

            i += 1

            # Gather title lines and find overview block (multi-line robust)
            while i < len(lines):
                nxt = lines[i].strip()
                if not nxt:
                    i += 1
                    continue

                # stop on next blocks
                if nxt.startswith("Call - "):
                    break
                if RE_TOPIC_ID.search(nxt):
                    break
                if RE_CALL_ID.search(nxt) and not RE_TOPIC_ID.search(nxt):
                    break

                # Try overview parse if line begins with action type
                tokens = nxt.split()
                if tokens and tokens[0] in ACTION_TYPES:
                    ov, new_i = _parse_overview_block(lines, i)
                    if ov:
                        pending_action_type = ov["action_type"]
                        pending_budget_total = ov["budget_eur_m"]
                        pending_per_min = ov["budget_per_project_min_eur_m"]
                        pending_per_max = ov["budget_per_project_max_eur_m"]
                        pending_projects = ov["projects"]
                        i = new_i
                        break  # after overview we can close the topic
                    # if failed, fall through and treat as title continuation

                # Avoid polluting titles with section headers
                if not nxt.startswith("Destination - "):
                    pending_title_parts.append(_norm(nxt))

                i += 1

            # Finalize this topic (even if overview wasn't found yet)
            flush_topic()
            continue

        i += 1

    # Final flush
    flush_topic()
    return out


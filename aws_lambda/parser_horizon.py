import re
from typing import Dict, List, Optional, Tuple

ACTION_TYPES = {
    "RIA",
    "IA",
    "CSA",
    "PCP",
    "PPI",
    "COFUND",
    "ERC",
    "MSCA",
    "EIC-PATHFINDER",
    "EIC-TRANSITION",
    "EIC-ACCELERATOR",
}
ACTION_TYPES_PATTERN = "(" + "|".join(sorted(ACTION_TYPES, key=len, reverse=True)) + ")"

RE_CALL_ID = re.compile(r"\bHORIZON-[A-Z0-9]+-\d{4}-\d{2}(?:-two-stage)?\b")

RE_TOPIC_ID = re.compile(
    r"\bHORIZON-[A-Z0-9]+-\d{4}-\d{2}-[A-Z0-9]+(?:-[A-Z0-9]+)*(?:-two-stage)?\b"
)

RE_OPENING = re.compile(r"Opening:\s*(.+)")
RE_DEADLINE = re.compile(r"Deadline\(s\):\s*(.+)")

RE_PAGE_MARKER = re.compile(r"^<<<PAGE\s+(\d+)>>>$")

RE_DOT_LEADER_PAGE = re.compile(r"\s\.{3,}\s*(\d{1,4})\s*$")

# Detect lines that are clearly a split identifier ending with '-'
RE_SPLIT_ID_LINE = re.compile(r"^HORIZON-[A-Z0-9]+(?:-[A-Z0-9]+)*-$")

# Headings to skip for topic_description capture
RE_SKIP_DESC = re.compile(
    r"^(Expected\s+Outcome|Scope|Specific\s+conditions|Type\s+of\s+action|Topic\s+description|Conditions|Expected\s+impact|Outcomes?)\b",
    flags=re.IGNORECASE,
)

def _norm(s: str) -> str:
    s = (s or "").strip()

    # Prevent soft hyphen removal from concatenating words
    s = re.sub(r"([A-Za-z])\u00ad([A-Za-z])", r"\1 \2", s)

    # Normalize weird hyphenation chars from PDFs
    s = s.replace("\u00ad", "")   # soft hyphen
    s = s.replace("￾", "-")       # seen as 'two￾stage' in some extractions
    s = s.replace("\u2010", "-")  # hyphen
    s = s.replace("\u2011", "-")  # non-breaking hyphen
    s = s.replace("\u2013", "-")  # en dash
    s = s.replace("\u2014", "-")  # em dash
    s = s.replace("\u2212", "-")  # minus
    s = s.replace("\u2019", "'")  # curly apostrophe

    return re.sub(r"\s+", " ", s).strip()


def _normalize_title_text(s: str) -> str:
    """
    Clean title text for topic headlines.
    - remove soft hyphens
    - join words split by hyphen + newline or accidental concatenation
    - ensure newlines become spaces
    - collapse whitespace
    """
    if not s:
        return ""

    s = re.sub(r"([A-Za-z])\u00ad([A-Za-z])", r"\1 \2", s)

    # Remove soft hyphen before other processing
    s = s.replace("\u00ad", "")

    # Join hyphenated line breaks: "Crimeprevention" happens when a newline is dropped; preserve the split
    s = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1-\2", s)
    # If newline without hyphen splits words, reintroduce space
    s = re.sub(r"(\w)\n(\w)", r"\1 \2", s)

    # Replace remaining newlines with spaces
    s = s.replace("\n", " ")

    # Remove stray spaces around hyphens that may remain
    s = _fix_inline_hyphen_spacing(s)

    # Targeted fixes for glued words seen in PDFs
    s = re.sub(r"(?i)\btopicon\b", "topic on", s)
    s = re.sub(r"(?i)\bimprovingcapabilities\b", "Improving capabilities", s)
    s = re.sub(r"(?i)\baccessibleand\b", "Accessible and", s)
    s = re.sub(r"(?i)\bcrimeprevention\b", "Crime prevention", s)

    def _split_glued(match: re.Match) -> str:
        left = match.group(1)
        right = match.group(2)
        suffixes = ("ing", "ed", "tion", "sion", "ment", "ness", "ity", "able", "ible", "al", "ic")
        if any(left.lower().endswith(suf) for suf in suffixes):
            return f"{left} {right}"
        return match.group(0)

    s = re.sub(r"(?i)\b([a-z]{4,})(on|in|of|for|with|and|to|by|from|at)\b", _split_glued, s)
    s = re.sub(r"(?i)\b([a-z]{4,})(and|for|on|in|with|between|across)([a-z]{3,})\b", r"\1 \2 \3", s)
    s = re.sub(r"(?i)\b([a-z]{4,})(capabilities|prevention|security|safety|resilience)\b", r"\1 \2", s)

    # Reintroduce spaces between camelCase-like joins (e.g., CrimePrevention)
    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s)

    # Collapse multiple spaces
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _fix_inline_hyphen_spacing(s: str) -> str:
    """
    Collapse stray spaces that appear before hyphens inside words, e.g. "gender -based" -> "gender-based".
    Does not touch spaced dashes like "transition - how".
    """
    if not s:
        return s
    return re.sub(r"(\w)\s+-([A-Za-z0-9])", r"\1-\2", s)

def _strip_dot_leader_page(s: str) -> Tuple[str, Optional[int]]:
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
    raw = _norm(ln.replace("Call - ", ""))
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

        parts = [p.strip() for p in inside.split("-") if p.strip()]
        if len(parts) >= 2:
            call_round = parts[-1].strip()

    return cluster or None, stage, call_round, page


def _derive_call_id_from_topic(topic_id: str) -> Optional[str]:
    m = re.match(r"^(HORIZON-[A-Z0-9]+-\d{4}-\d{2})-", topic_id)
    return m.group(1) if m else None


def _derive_call_round(topic_id: Optional[str]) -> Optional[str]:
    if not topic_id:
        return None
    m = re.search(r"HORIZON-[A-Z0-9]+-(\d{4})-(\d{2})", topic_id)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}"


def _derive_stage(topic_id: Optional[str], current_stage: Optional[str]) -> Optional[str]:
    if current_stage:
        return current_stage
    tid = (topic_id or "").lower()
    if "two-stage" in tid or "two stage" in tid:
        return "two-stage"
    if tid:
        return "single"
    return None


def _extract_trl(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"TRL\s*(\d)(?:\s*(?:[-–]|to)\s*(\d))?", text, flags=re.IGNORECASE)
    if not m:
        return None
    start = m.group(1)
    end = m.group(2)
    if end:
        return f"{start}-{end}"
    return start


def _merge_split_identifier_lines(lines: List[str]) -> List[str]:
    """
    Fix pypdf line breaks inside identifiers.

    Handles:
    1) strict case with trailing '-'
    2) loose case where 'HORIZON-...' is split without trailing '-'
    3) edge case: split occurs mid-token and the join needs removing a space around '-'
       (seen with ...-IND-02-two-stage)
    """
    out: List[str] = []
    i = 0

    def _try_join(a: str, b: str) -> Optional[str]:
        # Try several join strategies
        cands = [
            _norm(a + b),
            _norm(a + " " + b),
            _norm(a.rstrip() + b.lstrip()),
        ]
        # also remove " - " -> "-" because pypdf sometimes inserts spaces around hyphens
        cands += [_norm(c.replace(" - ", "-").replace("- ", "-").replace(" -", "-")) for c in cands]

        for c in cands:
            if RE_TOPIC_ID.search(c):
                return c
        return None

    while i < len(lines):
        ln = lines[i]

        # Case 1: trailing dash
        if RE_SPLIT_ID_LINE.search(ln) and i + 1 < len(lines):
            merged = _norm(ln + lines[i + 1].lstrip())
            out.append(merged)
            i += 2
            continue

        # Case 2/3: partial horizon id that becomes a full topic_id when joined with next line
        if "HORIZON-" in ln and i + 1 < len(lines):
            joined = _try_join(ln, lines[i + 1])
            if joined:
                out.append(joined)
                i += 2
                continue

        out.append(ln)
        i += 1

    return out



def _clean_overview_joined(s: str) -> str:
    """
    Removes footnote digits that break parsing, e.g.:
      "million)3" -> "million)"
    """
    s = _norm(s)
    s = re.sub(r"\)(\d{1,2})\b", ")", s)  # ")3" -> ")"
    s = re.sub(r"\b(EUR|million)\s+(\d{1,2})\b", r"\1", s, flags=re.IGNORECASE)
    return _norm(s)


def _parse_overview_block(lines: List[str], start_i: int) -> Tuple[Optional[Dict], int]:
    """
    Multi-line parse of overview row (may be split across lines).
    Supports:
      - "9.00 to 10.00 2"
      - "Around 10.00 4"
      - CARE-03-like: "Around 4 ... 10.00" (projects near Around, amount later)
    """
    i = start_i
    buf: List[str] = []

    for _ in range(12):
        if i >= len(lines):
            break

        ln = _norm(lines[i])
        if not ln:
            i += 1
            continue

        # stop on new blocks
        if RE_TOPIC_ID.search(ln) or ln.startswith("Call - "):
            break
        if RE_CALL_ID.search(ln) and not RE_TOPIC_ID.search(ln):
            break

        buf.append(ln)
        joined = _clean_overview_joined(" ".join(buf))

        m = re.match(rf"^{ACTION_TYPES_PATTERN}\s+(.*)$", joined)
        if not m:
            i += 1
            continue

        action = m.group(1)
        rest = m.group(2)

        total_m = re.search(r"\b(\d{1,4}(?:\.\d{1,2})?)\b", rest)
        if not total_m:
            i += 1
            continue
        total = float(total_m.group(1))

        # Range: "... 9.00 to 10.00 2"
        rp = re.search(
            r"\b(\d{1,4}(?:\.\d{1,2})?)\s+to\s+(\d{1,4}(?:\.\d{1,2})?)\s+(\d{1,3})\b",
            rest,
        )
        if rp:
            return ({
                "action_type": action,
                "budget_eur_m": total,
                "budget_per_project_min_eur_m": float(rp.group(1)),
                "budget_per_project_max_eur_m": float(rp.group(2)),
                "projects": int(rp.group(3)),
            }, i + 1)

        # Around clean: "... Around 10.00 4"
        ap = re.search(
            r"\bAround\s+(\d{1,4}(?:\.\d{1,2})?)\s+(\d{1,3})\b",
            rest,
            flags=re.IGNORECASE,
        )
        if ap:
            p = float(ap.group(1))
            return ({
                "action_type": action,
                "budget_eur_m": total,
                "budget_per_project_min_eur_m": p,
                "budget_per_project_max_eur_m": p,
                "projects": int(ap.group(2)),
            }, i + 1)

        # Around messy (CARE-03 style): "... Around 4 ... 10.00"
        m_ap_int = re.search(r"\bAround\s+(\d{1,3})\b", rest, flags=re.IGNORECASE)
        if m_ap_int:
            projects = int(m_ap_int.group(1))
            floats = re.findall(r"\b(\d{1,4}(?:\.\d{1,2})?)\b", rest)
            p = None
            for f in reversed(floats):
                try:
                    val = float(f)
                except Exception:
                    continue
                if val != total:
                    p = val
                    break
            if p is not None:
                return ({
                    "action_type": action,
                    "budget_eur_m": total,
                    "budget_per_project_min_eur_m": p,
                    "budget_per_project_max_eur_m": p,
                    "projects": projects,
                }, i + 1)

        i += 1

    return None, start_i


def _extract_topic_body(lines: List[str], start_i: int, max_lines: int = 140) -> str:
    """
    Extract a body snippet for the topic starting at index start_i (right after topic line).
    Stops at next topic/call marker. Returns plain text (already normalized).
    """
    buf: List[str] = []
    i = start_i
    taken = 0

    while i < len(lines) and taken < max_lines:
        ln = _norm(lines[i])
        if not ln:
            i += 1
            continue

        # stop on next blocks
        if ln.startswith("Call - "):
            break
        if RE_TOPIC_ID.search(ln):
            break
        if RE_CALL_ID.search(ln) and not RE_TOPIC_ID.search(ln):
            break

        # keep useful lines; drop pure table noise if you want (optional)
        buf.append(ln)
        taken += 1
        i += 1

    return "\n".join(buf).strip()


def _extract_topic_description(body_text: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not body_text:
        return None, None

    lines = [_norm(ln) for ln in str(body_text).splitlines() if _norm(ln)]
    if not lines:
        return None, None

    def _is_stop(ln: str) -> bool:
        low = ln.lower()
        stop_prefixes = (
            "specific conditions",
            "type of action",
            "general conditions",
            "general conditions relating to this call",
            "eligibility conditions",
            "admissibility conditions",
            "conditions are described in general",
            "the conditions are described in general",
            "topic conditions and documents",
        )
        return any(low.startswith(p) for p in stop_prefixes)

    expected_idx = None
    scope_idx = None
    for idx, ln in enumerate(lines):
        low = ln.lower()
        if expected_idx is None and low.startswith("expected outcome"):
            expected_idx = idx
            continue
        if scope_idx is None and low.startswith("scope"):
            scope_idx = idx
            continue

    def _collect(start_idx: Optional[int], end_idx: Optional[int]) -> Optional[str]:
        if start_idx is None or start_idx >= len(lines):
            return None
        collected: List[str] = []
        i = start_idx
        while i < len(lines):
            ln = lines[i]
            low = ln.lower()
            if i == start_idx and (low.startswith("expected outcome") or low.startswith("scope")):
                i += 1
                continue
            if end_idx is not None and i >= end_idx:
                break
            if _is_stop(ln):
                break
            collected.append(ln)
            i += 1
        text = "\n".join(collected).strip()
        return text or None

    expected_end = scope_idx if scope_idx is not None else None
    expected_text = _collect(expected_idx, expected_end)
    scope_text = _collect(scope_idx, None)

    if not expected_text and not scope_text:
        return None, None
    return expected_text, scope_text


def parse_calls(text: str) -> List[Dict]:
    # Normalize every line early (important for weird hyphens)
    raw_lines = [_norm(ln) for ln in (text or "").splitlines() if _norm(ln)]
    lines = _merge_split_identifier_lines(raw_lines)

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
    pending_description_parts: List[str] = []
    pending_body: Optional[str] = None
    pending_page: Optional[int] = None

    pending_action_type: Optional[str] = None
    pending_budget_total: Optional[float] = None
    pending_per_min: Optional[float] = None
    pending_per_max: Optional[float] = None
    pending_projects: Optional[int] = None

    best_by_topic: Dict[str, Dict] = {}

    def score(r: Dict) -> int:
        # Prefer rows with overview fields present (avoid TOC duplicates)
        return sum(
            1 for k in (
                "action_type",
                "budget_eur_m",
                "projects",
                "budget_per_project_min_eur_m",
                "budget_per_project_max_eur_m",
            )
            if r.get(k) not in (None, "", 0)
        )

    def _stop_title(ln: str) -> bool:
        if not ln:
            return False
        stop_keywords = (
            "the director-general",
            "all deadlines are",
            "brussels local time",
            "opening date",
            "deadline date",
            "funding:",
            "funding opportunities",
            "expected outcome",
            "expected outcomes",
            "expected impact",
            "deadline(s)",
            "annex",
            "eligibility conditions",
            "the conditions are described in general",
            "conditions are described in general",
            "admissibility conditions",
            "general conditions relating to this call",
            "topic conditions and documents",
        )
        low = ln.lower()
        return any(k in low for k in stop_keywords)

    def flush_topic():
        nonlocal pending_topic_id, pending_title_parts, pending_description_parts, pending_body, pending_page
        nonlocal pending_action_type, pending_budget_total, pending_per_min, pending_per_max, pending_projects
        nonlocal current_call_id

        if not pending_topic_id:
            return

        title_raw = _normalize_title_text("\n".join(pending_title_parts))
        title_clean, title_page = _strip_dot_leader_page(title_raw)
        title_clean = _fix_inline_hyphen_spacing(title_clean)

        page = current_page or pending_page or title_page or current_cluster_page
        expected_text, scope_text = _extract_topic_description(pending_body)
        topic_desc_parts: List[str] = []
        if expected_text:
            topic_desc_parts.append(f"Expected Outcome:\n{expected_text}")
        if scope_text:
            topic_desc_parts.append(f"Scope:\n{scope_text}")
        topic_desc = "\n\n".join(topic_desc_parts).strip() or None
        if not topic_desc:
            topic_desc = "\n".join(pending_description_parts).strip() or None
        trl_val = _extract_trl("\n".join(filter(None, [pending_body, topic_desc])))

        if not current_call_id:
            current_call_id = _derive_call_id_from_topic(pending_topic_id)

        row = {
            "cluster": current_cluster,
            "stage": _derive_stage(pending_topic_id, current_stage),
            "call_round": current_call_round or _derive_call_round(pending_topic_id),
            "page": page,

            "call_id": current_call_id,
            "topic_id": pending_topic_id,
            "topic_title": title_clean,
            "topic_description": topic_desc,

            "action_type": pending_action_type,
            "opening_date": current_opening,
            "deadline_date": current_deadline,

            "budget_eur_m": pending_budget_total,
            "projects": pending_projects,
            "budget_per_project_min_eur_m": pending_per_min,
            "budget_per_project_max_eur_m": pending_per_max,

            "trl": trl_val,

            "topic_body": pending_body,
        }

        prev = best_by_topic.get(pending_topic_id)
        if prev is None or score(row) > score(prev):
            best_by_topic[pending_topic_id] = row

        # reset pending
        pending_topic_id = None
        pending_title_parts = []
        pending_description_parts = []
        pending_body = None
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

        # Cluster
        if ln.startswith("Call - "):
            flush_topic()
            cluster, stage, call_round, page = _parse_cluster_line(ln)
            current_cluster = cluster
            current_stage = stage
            current_call_round = call_round
            current_cluster_page = page

            current_call_id = None
            current_opening = None
            current_deadline = None

            i += 1
            continue

        # Call id
        m_call = RE_CALL_ID.search(ln)
        if m_call and not RE_TOPIC_ID.search(ln):
            current_call_id = m_call.group(0)
            i += 1
            continue

        # Dates
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

        # Topic
        m_topic = RE_TOPIC_ID.search(ln)
        if m_topic:
            flush_topic()

            pending_topic_id = m_topic.group(0)
            pending_page = current_page

            cleaned_line, _ = _strip_dot_leader_page(ln)
            after = cleaned_line.split(":", 1)
            pending_title_parts = [_norm(after[1])] if len(after) == 2 else []

            i += 1  # passa alla riga DOPO il topic_id
            # capture body snippet for optional GPT description (starting after topic line)
            pending_body = _extract_topic_body(lines, i, max_lines=140)

            # Gather title until we parse overview
            while i < len(lines):
                nxt = _norm(lines[i])
                if not nxt:
                    i += 1
                    continue

                if nxt.startswith("Call - "):
                    break
                if RE_TOPIC_ID.search(nxt):
                    break
                if RE_CALL_ID.search(nxt) and not RE_TOPIC_ID.search(nxt):
                    break

                if _stop_title(nxt):
                    break

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

                        # --- Extract short topic_description (2–3 real lines) ---
                        desc_lines = 0
                        while i < len(lines) and desc_lines < 3:
                            tail = _norm(lines[i])

                            if not tail:
                                i += 1
                                continue

                            # stop on new logical blocks
                            if (
                                tail.startswith("Destination - ")
                                or tail.startswith("Call - ")
                                or RE_TOPIC_ID.search(tail)
                                or tail.split()[0] in ACTION_TYPES
                            ):
                                break

                            # skip headings
                            if RE_SKIP_DESC.match(tail):
                                i += 1
                                continue

                            pending_description_parts.append(tail)
                            desc_lines += 1
                            i += 1

                        # EXTRA: capture trailing title fragments / missing per-project amount on next 1-2 lines
                        for _ in range(2):
                            if i >= len(lines):
                                break
                            tail = _norm(lines[i])

                            if not tail or tail.startswith("Destination - ") or tail.startswith("Call - ") or RE_TOPIC_ID.search(tail):
                                break

                            if _stop_title(tail):
                                break

                            m_float_end = re.search(r"(.*)\b(\d{1,4}(?:\.\d{1,2})?)\s*$", tail)
                            if m_float_end:
                                left = _norm(m_float_end.group(1))
                                val = float(m_float_end.group(2))

                                if left and (not left.split() or left.split()[0] not in ACTION_TYPES):
                                    # avoid capturing pure headings
                                    pending_title_parts.append(left)

                                # if we missed per-project (rare), fill it
                                if pending_per_min is None or pending_per_max is None:
                                    pending_per_min = val
                                    pending_per_max = val
                            else:
                                if any(c.isalpha() for c in tail):
                                    pending_title_parts.append(tail)

                            i += 1

                        break

                if (
                    not nxt.startswith("Destination - ")
                    and not pending_description_parts
                    and not RE_SKIP_DESC.match(nxt)
                    and not _stop_title(nxt)
                ):
                    pending_title_parts.append(nxt)

                i += 1

            flush_topic()
            continue

        i += 1

    flush_topic()

    rows = list(best_by_topic.values())
    rows.sort(key=lambda r: (r.get("call_id") or "", r.get("topic_id") or ""))
    return rows

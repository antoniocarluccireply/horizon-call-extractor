import re
from typing import List, Dict, Optional

def _norm(text: str) -> str:
    t = text.replace("\u00ad", "")  # soft hyphen
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t

def _find_cluster(text: str) -> Optional[str]:
    m = re.search(r"(Cluster\s+\d+\s*[–-]\s*[^\n]+)", text, re.IGNORECASE)
    return m.group(1).strip() if m else None

def _near(text: str, idx: int, left: int = 600, right: int = 1400) -> str:
    return text[max(0, idx-left): min(len(text), idx+right)]

def parse_calls(text: str) -> List[Dict]:
    """
    Estrae SOLO ciò che trova nel documento (niente invenzioni).
    Prima iterazione: lavora per topic_id e cattura metadati vicini.
    """
    t = _norm(text)
    cluster = _find_cluster(t)

    topic_pat = re.compile(r"\bHORIZON[-A-Z0-9_]+-\d{4}[-A-Z0-9_]+?\b")
    topic_ids = list(dict.fromkeys(topic_pat.findall(t)))

    rows: List[Dict] = []

    for topic_id in topic_ids:
        idx = t.find(topic_id)
        w = _near(t, idx)

        # Titolo: prova a prenderlo dalla riga del topic id o dalla successiva
        topic_title = None
        lines = w.splitlines()
        for i, line in enumerate(lines[:40]):
            if topic_id in line:
                cand = line.replace(topic_id, "").strip(" -–:\t")
                if len(cand) >= 8:
                    topic_title = cand
                elif i + 1 < len(lines) and len(lines[i+1].strip()) >= 8:
                    topic_title = lines[i+1].strip()
                break

        # Action type (RIA/IA/CSA)
        action_type = None
        m = re.search(r"\b(RIA|IA|CSA)\b", w)
        if m:
            action_type = m.group(1)

        # TRL (es. TRL 5-6)
        trl = None
        m = re.search(r"\bTRL\s*([0-9](?:\s*[-–]\s*[0-9])?)\b", w, re.IGNORECASE)
        if m:
            trl = m.group(1).replace(" ", "")

        # Budget: EUR xx million / EUR xx m
        budget_eur_m = None
        m = re.search(r"\bEUR\s*([0-9]+(?:\.[0-9]+)?)\s*(?:m|million)\b", w, re.IGNORECASE)
        if m:
            budget_eur_m = m.group(1)

        # Opening / Deadline (pattern generico: “Opening date …” / “Deadline date …”)
        opening_date = None
        deadline_date = None
        m = re.search(r"Opening(?: date)?\s*[:\-]?\s*([0-9]{1,2}\s+\w+\s+20[0-9]{2}|20[0-9]{2}-[0-9]{2}-[0-9]{2})", w, re.IGNORECASE)
        if m:
            opening_date = m.group(1)
        m = re.search(r"Deadline(?: date)?\s*[:\-]?\s*([0-9]{1,2}\s+\w+\s+20[0-9]{2}|20[0-9]{2}-[0-9]{2}-[0-9]{2})", w, re.IGNORECASE)
        if m:
            deadline_date = m.group(1)

        rows.append({
            "cluster": cluster,
            "call_id": None,          # lo agganciamo nella prossima iterazione (serve pattern “Call: …”)
            "topic_id": topic_id,
            "topic_title": topic_title,
            "action_type": action_type,
            "trl": trl,
            "budget_eur_m": budget_eur_m,
            "opening_date": opening_date,
            "deadline_date": deadline_date,
        })

    return rows

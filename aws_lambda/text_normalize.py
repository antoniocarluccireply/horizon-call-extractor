import re
from typing import Iterable

INVISIBLE_PDF_CHARS = re.compile(r"[\u00ad\ufffd\ufffe\uffff]")
HYPHEN_LINE_BREAK = re.compile(r"([A-Za-z0-9])[\-\u2010\u2011\u2012\u2013\u2014]\s*\n\s*([A-Za-z0-9])")

_SUFFIXES = {
    "ing",
    "tion",
    "sion",
    "ment",
    "ments",
    "ness",
    "ity",
    "able",
    "ible",
    "al",
    "ic",
    "ive",
    "ous",
    "ants",
    "ant",
    "ers",
    "er",
    "ed",
    "ly",
    "ways",
    "way",
    "ism",
    "ist",
    "ation",
    "ations",
    "ions",
    "ent",
    "ents",
}

_MIDDLE_JOINERS = {"and", "or"}


def _merge_case(first: str, second: str) -> str:
    if first[:1].isupper() and first[1:].islower() and second[:1].isupper() and second[1:].islower():
        second = second.lower()
    return f"{first}{second}"


def _collapse_broken_word_fragments(text: str) -> str:
    def _merge_triplet(match: re.Match) -> str:
        first = match.group(1)
        middle = match.group(2)
        last = match.group(3)
        if len(first) < 5 or middle.lower() not in _MIDDLE_JOINERS or last.lower() not in _SUFFIXES:
            return match.group(0)
        tail = f"{middle}{last}"
        if first[:1].isupper() and first[1:].islower():
            tail = tail.lower()
        return f"{first}{tail}"

    def _merge_pair(match: re.Match) -> str:
        first = match.group(1)
        second = match.group(2)
        if second.lower() not in _SUFFIXES or len(first) < 3:
            return match.group(0)
        return _merge_case(first, second)

    text = re.sub(r"\b([A-Za-z]{4,})\s+(and|or)\s+([A-Za-z]{2,6})\b", _merge_triplet, text, flags=re.IGNORECASE)
    text = re.sub(r"\b([A-Za-z]{3,})\s+([A-Za-z]{2,6})\b", _merge_pair, text)
    return text


def normalize_pdf_text(value: str, *, preserve_newlines: bool = False) -> str:
    if not value:
        return ""
    text = str(value).replace("\u00a0", " ")
    text = INVISIBLE_PDF_CHARS.sub("", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = HYPHEN_LINE_BREAK.sub(r"\1-\2", text)
    if preserve_newlines:
        lines: Iterable[str] = (ln.strip() for ln in re.split(r"\n", text))
        cleaned_lines = [_collapse_broken_word_fragments(ln) if ln else "" for ln in lines]
        text = "\n".join(cleaned_lines)
        text = _apply_known_fixes(text)
        return text.strip()
    text = re.sub(r"\s*\n+\s*", " ", text)
    text = _collapse_broken_word_fragments(text)
    text = _apply_known_fixes(text)
    return re.sub(r"\s+", " ", text).strip()


def _apply_known_fixes(text: str) -> str:
    text = re.sub(r"\bresp\s*on\s*ses\b", "responses", text, flags=re.IGNORECASE)
    text = re.sub(r"\bresp\s*on\s*ders\b", "responders", text, flags=re.IGNORECASE)
    text = re.sub(r"\benvir\s*on\s*ments\b", "environments", text, flags=re.IGNORECASE)
    text = re.sub(r"\bpers\s*on\s*alised\b", "personalised", text, flags=re.IGNORECASE)
    text = re.sub(r"\bpers\s*on\s*alized\b", "personalized", text, flags=re.IGNORECASE)
    return text

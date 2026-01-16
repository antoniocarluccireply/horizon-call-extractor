import { normalizeTopicText } from "./normalizeTopicText";

export type InlineToken =
  | { kind: "text"; text: string }
  | { kind: "ref"; index: number; rawId: string }
  | { kind: "label"; text: string };

export type Block =
  | { kind: "paragraph"; tokens: InlineToken[] }
  | { kind: "list"; items: InlineToken[][] }
  | {
      kind: "references";
      items: { index: number; rawId: string; url?: string; text?: string }[];
    };

type FootnoteState = {
  map: Map<string, number>;
  order: string[];
};

const bracketRefRegex = /\[(\d{1,4})\]/g;
const attachedRefRegex = /([A-Za-z\)])(\d{1,2})(?=[^0-9A-Za-z]|$)/g;
const urlRegex = /https?:\/\/\S+/gi;
const referenceLineRegex =
  /^\s*(?:\[(\d{1,4})\]|(\d{1,4}))\s+(.+?)\s*(https?:\/\/\S+)\s*$/i;
const referenceUrlOnlyRegex = /^\s*(https?:\/\/\S+)\s*$/i;
const labelOnlyRegex = /^(Expected Outcome|Scope):?\s*$/i;
const labelInlineRegex = /^(Expected Outcome|Scope)\s*:/i;

function getFootnoteIndex(rawId: string, state: FootnoteState): number {
  const existing = state.map.get(rawId);
  if (existing) {
    return existing;
  }
  const nextIndex = state.order.length + 1;
  state.map.set(rawId, nextIndex);
  state.order.push(rawId);
  return nextIndex;
}

function cleanUrl(url: string): string {
  return url.replace(/[),.;:]+$/g, "");
}

function ensureReferenceEntry(
  rawId: string,
  data: { url?: string; text?: string },
  map: Map<string, { url?: string; text?: string }>,
): void {
  const existing = map.get(rawId);
  if (!existing) {
    map.set(rawId, data);
    return;
  }
  map.set(rawId, {
    url: existing.url || data.url,
    text: existing.text || data.text,
  });
}

function extractReferenceLine(line: string): {
  rawId: string;
  url: string;
  text?: string;
} | null {
  const trimmed = line.trim();
  if (!trimmed) {
    return null;
  }
  const match = trimmed.match(referenceLineRegex);
  if (match) {
    const rawId = match[1] || match[2];
    const url = cleanUrl(match[4]);
    return { rawId, url, text: match[3]?.trim() || undefined };
  }
  const urlOnly = trimmed.match(referenceUrlOnlyRegex);
  if (urlOnly) {
    const url = cleanUrl(urlOnly[1]);
    return { rawId: url, url };
  }
  return null;
}

function isSentenceContinuation(prev: string, next: string): boolean {
  if (/[.!?;:]$/.test(prev)) {
    return false;
  }
  if (/^[a-z]/.test(next)) {
    return true;
  }
  return /^[("“‘’\[]/.test(next);
}

function mergeLines(lines: string[]): string[] {
  const merged: string[] = [];
  for (let i = 0; i < lines.length; i += 1) {
    const raw = lines[i].replace(/[ \t]+$/g, "");
    if (!raw.trim()) {
      merged.push("");
      continue;
    }
    let current = raw.trim();
    while (i + 1 < lines.length) {
      const next = lines[i + 1];
      const nextTrim = next.trim();
      if (!nextTrim) {
        break;
      }
      if (/-$/.test(current) && /^[\p{L}]/u.test(nextTrim)) {
        current = current.replace(/-$/, "-") + nextTrim;
        i += 1;
        continue;
      }
      if (labelOnlyRegex.test(current)) {
        const labelText = current.replace(/:\s*$/, ":");
        current = `${labelText} ${nextTrim}`;
        i += 1;
        continue;
      }
      if (isSentenceContinuation(current, nextTrim)) {
        current = `${current} ${nextTrim}`;
        i += 1;
        continue;
      }
      break;
    }
    merged.push(current);
  }
  return merged;
}

function tokenizeText(
  text: string,
  state: FootnoteState,
  referenceMap: Map<string, { url?: string; text?: string }>,
): InlineToken[] {
  const tokens: InlineToken[] = [];
  let index = 0;
  while (index < text.length) {
    urlRegex.lastIndex = index;
    bracketRefRegex.lastIndex = index;
    attachedRefRegex.lastIndex = index;
    const urlMatch = urlRegex.exec(text);
    const bracketMatch = bracketRefRegex.exec(text);
    const attachedMatch = attachedRefRegex.exec(text);
    const candidates = [urlMatch, bracketMatch, attachedMatch].filter(
      Boolean,
    ) as RegExpExecArray[];
    if (!candidates.length) {
      const remaining = text.slice(index);
      if (remaining) {
        tokens.push({ kind: "text", text: remaining });
      }
      break;
    }
    let nextMatch = candidates[0];
    for (const candidate of candidates) {
      if (candidate.index < nextMatch.index) {
        nextMatch = candidate;
      }
    }
    if (nextMatch.index > index) {
      tokens.push({ kind: "text", text: text.slice(index, nextMatch.index) });
    }
    if (nextMatch === urlMatch) {
      let url = nextMatch[0];
      let trailing = "";
      while (/[),.;:]+$/.test(url)) {
        trailing = url.slice(-1) + trailing;
        url = url.slice(0, -1);
      }
      const clean = cleanUrl(url);
      ensureReferenceEntry(clean, { url: clean }, referenceMap);
      tokens.push({
        kind: "ref",
        index: getFootnoteIndex(clean, state),
        rawId: clean,
      });
      if (trailing) {
        tokens.push({ kind: "text", text: trailing });
      }
      index = nextMatch.index + nextMatch[0].length;
      continue;
    }
    if (nextMatch === bracketMatch) {
      const rawId = nextMatch[1];
      tokens.push({
        kind: "ref",
        index: getFootnoteIndex(rawId, state),
        rawId,
      });
      index = nextMatch.index + nextMatch[0].length;
      continue;
    }
    const rawId = nextMatch[2];
    const prefixEnd = nextMatch.index + 1;
    const leadingText = text.slice(index, prefixEnd);
    if (leadingText) {
      tokens.push({ kind: "text", text: leadingText });
    }
    tokens.push({
      kind: "ref",
      index: getFootnoteIndex(rawId, state),
      rawId,
    });
    index = prefixEnd + rawId.length;
  }
  return tokens;
}

function tokenizeInline(
  text: string,
  state: FootnoteState,
  referenceMap: Map<string, { url?: string; text?: string }>,
): InlineToken[] {
  const tokens: InlineToken[] = [];
  const labelMatch = text.match(labelInlineRegex);
  if (labelMatch) {
    const labelText = text.slice(0, labelMatch[0].length).trim();
    tokens.push({ kind: "label", text: labelText });
    const remainder = text.slice(labelMatch[0].length).trimStart();
    if (remainder) {
      tokens.push(
        ...tokenizeText(` ${remainder}`, state, referenceMap),
      );
    }
    return tokens;
  }
  return tokenizeText(text, state, referenceMap);
}

export function topicDescriptionBlocks(input: string): Block[] {
  const normalized = normalizeTopicText(input);
  if (!normalized) {
    return [];
  }

  const lines = mergeLines(normalized.split("\n"));
  const referenceMap = new Map<string, { url?: string; text?: string }>();
  const workingLines: string[] = [];

  lines.forEach((line) => {
    const trimmed = line.trim();
    if (!trimmed) {
      workingLines.push(line);
      return;
    }
    const reference = extractReferenceLine(trimmed);
    if (reference) {
      ensureReferenceEntry(
        reference.rawId,
        { url: reference.url, text: reference.text },
        referenceMap,
      );
      return;
    }
    workingLines.push(line);
  });
  const blocks: Block[] = [];
  let paragraphLines: string[] = [];
  let listItems: InlineToken[][] = [];
  const footnoteState: FootnoteState = { map: new Map(), order: [] };

  const flushParagraph = () => {
    if (!paragraphLines.length) {
      return;
    }
    const text = paragraphLines.join(" ").replace(/\s+/g, " ").trim();
    paragraphLines = [];
    if (!text) {
      return;
    }
    blocks.push({
      kind: "paragraph",
      tokens: tokenizeInline(text, footnoteState, referenceMap),
    });
  };

  const flushList = () => {
    if (!listItems.length) {
      return;
    }
    blocks.push({ kind: "list", items: listItems });
    listItems = [];
  };

  workingLines.forEach((line) => {
    const trimmed = line.trim();
    if (!trimmed) {
      flushParagraph();
      flushList();
      return;
    }
    const bulletMatch = trimmed.match(/^([•\-–])\s+(.*)$/);
    if (bulletMatch) {
      flushParagraph();
      listItems.push(
        tokenizeInline(bulletMatch[2].trim(), footnoteState, referenceMap),
      );
      return;
    }
    flushList();
    paragraphLines.push(trimmed);
  });

  flushParagraph();
  flushList();

  if (footnoteState.order.length) {
    const items = footnoteState.order.map((rawId, index) => ({
      index: index + 1,
      rawId,
      url: referenceMap.get(rawId)?.url,
      text: referenceMap.get(rawId)?.text,
    }));
    blocks.push({ kind: "references", items });
  }

  return blocks;
}

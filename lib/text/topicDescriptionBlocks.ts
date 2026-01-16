import { normalizeTopicText } from "./normalizeTopicText";

export type InlineToken =
  | { kind: "text"; text: string }
  | { kind: "ref"; index: number; rawId: string };

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

const footnoteRegex = /([A-Za-z)])(\d{3,})(?=[^0-9A-Za-z]|$)/g;
const referenceLineRegex =
  /^\s*\[?(\d{3,})\]?\s*(?:(.+?)\s*:)?\s*(https?:\/\/\S+)\s*$/i;

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

function tokenizeInline(text: string, state: FootnoteState): InlineToken[] {
  const tokens: InlineToken[] = [];
  let lastIndex = 0;
  let match: RegExpExecArray | null;
  footnoteRegex.lastIndex = 0;
  while ((match = footnoteRegex.exec(text)) !== null) {
    const prefixEnd = match.index + 1;
    const rawId = match[2];
    const leadingText = text.slice(lastIndex, prefixEnd);
    if (leadingText) {
      tokens.push({ kind: "text", text: leadingText });
    }
    tokens.push({
      kind: "ref",
      index: getFootnoteIndex(rawId, state),
      rawId,
    });
    lastIndex = prefixEnd + rawId.length;
  }
  if (lastIndex < text.length) {
    tokens.push({ kind: "text", text: text.slice(lastIndex) });
  }
  return tokens;
}

export function topicDescriptionBlocks(input: string): Block[] {
  const normalized = normalizeTopicText(input);
  if (!normalized) {
    return [];
  }

  const lines = normalized.split("\n");
  const referenceMap = new Map<string, { url?: string; text?: string }>();
  const workingLines: string[] = [];

  lines.forEach((line) => {
    const trimmed = line.trim();
    if (!trimmed) {
      workingLines.push(line);
      return;
    }
    const match = trimmed.match(referenceLineRegex);
    if (match) {
      const [, rawId, text, url] = match;
      referenceMap.set(rawId, { url, text: text?.trim() || undefined });
      return;
    }
    if (/^\d{3,}\s+https?:\/\/\S+/i.test(trimmed)) {
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
      tokens: tokenizeInline(text, footnoteState),
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
      listItems.push(tokenizeInline(bulletMatch[2].trim(), footnoteState));
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

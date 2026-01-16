import { normalizeTopicText } from "./normalizeTopicText";

export type InlineToken =
  | { kind: "text"; text: string }
  | { kind: "footnoteRef"; index: number };

export type Block =
  | { kind: "paragraph"; tokens: InlineToken[] }
  | { kind: "list"; items: InlineToken[][] }
  | {
      kind: "references";
      items: { index: number; rawId: string; url?: string }[];
    };

type FootnoteState = {
  map: Map<string, number>;
  order: string[];
};

const footnoteRegex = /([A-Za-z)])(\d{3,})(?=[^0-9A-Za-z]|$)/g;

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
    tokens.push({ kind: "footnoteRef", index: getFootnoteIndex(rawId, state) });
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
  const referenceLines: string[] = [];
  let endIndex = lines.length - 1;
  while (endIndex >= 0 && !lines[endIndex].trim()) {
    endIndex -= 1;
  }
  while (endIndex >= 0) {
    const trimmed = lines[endIndex].trim();
    if (!trimmed) {
      endIndex -= 1;
      continue;
    }
    if (/^\d+\s+https?:\/\/\S+/.test(trimmed)) {
      referenceLines.unshift(trimmed);
      endIndex -= 1;
      continue;
    }
    break;
  }

  const referenceMap = new Map<string, string>();
  referenceLines.forEach((line) => {
    const match = line.match(/^(\d+)\s+(https?:\/\/\S+)/);
    if (!match) {
      return;
    }
    referenceMap.set(match[1], match[2]);
  });

  const workingLines = lines.slice(0, endIndex + 1);
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
    blocks.push({ kind: "paragraph", tokens: tokenizeInline(text, footnoteState) });
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
      url: referenceMap.get(rawId),
    }));
    blocks.push({ kind: "references", items });
  }

  return blocks;
}

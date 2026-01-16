import { topicDescriptionBlocks } from "../text/topicDescriptionBlocks";
import { normalizeTopicText } from "../text/normalizeTopicText";

export type PortalTopicDescription = {
  url: string;
  rawText: string;
  blocks: ReturnType<typeof topicDescriptionBlocks>;
};

const topicHeadingRegex =
  /<h([1-6])[^>]*>[\s\S]*?Topic description[\s\S]*?<\/h\1>/i;
const nextHeadingRegex = /<h[1-6][^>]*>/gi;

function decodeHtmlEntities(input: string): string {
  return input
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, "\"")
    .replace(/&apos;/gi, "'")
    .replace(/&#39;/gi, "'");
}

function stripSectionHtml(sectionHtml: string): string {
  const withLineBreaks = sectionHtml
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/?p[^>]*>/gi, "\n\n")
    .replace(/<\/?div[^>]*>/gi, "\n")
    .replace(/<li[^>]*>/gi, "\n• ")
    .replace(/<\/li>/gi, "")
    .replace(/<\/?(ul|ol)[^>]*>/gi, "\n")
    .replace(/<\/?strong[^>]*>/gi, "")
    .replace(/<\/?em[^>]*>/gi, "")
    .replace(/<\/?span[^>]*>/gi, "")
    .replace(/<\/?a[^>]*>/gi, "")
    .replace(/<\/?h[1-6][^>]*>/gi, "\n\n");

  const withoutTags = withLineBreaks.replace(/<[^>]+>/g, "");

  return decodeHtmlEntities(withoutTags)
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]+/g, " ")
    .replace(/^[ \t]+/gm, "")
    .trim();
}

function extractTopicSection(html: string): string | null {
  const cleaned = html
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?<\/style>/gi, "");
  const match = cleaned.match(topicHeadingRegex);
  if (!match || match.index === undefined) {
    return null;
  }
  const startIndex = match.index + match[0].length;
  nextHeadingRegex.lastIndex = startIndex;
  const nextHeadingMatch = nextHeadingRegex.exec(cleaned);
  const endIndex = nextHeadingMatch?.index ?? cleaned.length;
  return cleaned.slice(startIndex, endIndex);
}

export async function fetchPortalTopicDescription(
  url: string,
): Promise<PortalTopicDescription | null> {
  const portalUrl = new URL(url);
  if (!portalUrl.hostname.endsWith("ec.europa.eu")) {
    return null;
  }
  const response = await fetch(portalUrl.toString());
  if (!response.ok) {
    return null;
  }
  const html = await response.text();
  const section = extractTopicSection(html);
  if (!section) {
    return null;
  }
  const rawText = normalizeTopicText(stripSectionHtml(section));
  if (!rawText) {
    return null;
  }
  return {
    url: portalUrl.toString(),
    rawText,
    blocks: topicDescriptionBlocks(rawText),
  };
}

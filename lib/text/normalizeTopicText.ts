export function normalizeTopicText(input: string): string {
  if (!input) {
    return "";
  }

  let normalized = String(input)
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .replace(/[\u00ad\uFFFD]/g, "")
    .replace(/[\u2010\u2011\u2212]/g, "-")
    .replace(/&quot;/g, "\"")
    .replace(/&apos;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/[ \t]+$/gm, "");

  let prev: string | null = null;
  while (prev !== normalized) {
    prev = normalized;
    normalized = normalized.replace(
      /([\p{L}])\s*-\s*([\p{L}])/gu,
      "$1-$2",
    );
  }

  normalized = normalized.replace(/([\p{L}])-\n([\p{L}])/gu, "$1-$2");

  return normalized.trim();
}

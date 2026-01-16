export function normalizeTopicText(input: string): string {
  if (!input) {
    return "";
  }

  return String(input)
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+$/gm, "")
    .replace(/\b([A-Za-z])\s*-\s*([A-Za-z])\b/g, "$1-$2")
    .trim();
}

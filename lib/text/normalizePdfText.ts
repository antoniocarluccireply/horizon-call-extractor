const INVISIBLE_PDF_CHARS = /[\u00ad\ufffd\ufffe\uffff]/g;
const HYPHEN_LINE_BREAK = /([A-Za-z0-9])[\-\u2010\u2011\u2012\u2013\u2014]\s*\n\s*([A-Za-z0-9])/g;

const SUFFIXES = new Set([
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
]);

const MIDDLE_JOINERS = new Set(["and", "or"]);

function mergeCase(first: string, second: string): string {
  if (
    first.length > 1 &&
    second.length > 1 &&
    first[0] === first[0].toUpperCase() &&
    first.slice(1) === first.slice(1).toLowerCase() &&
    second[0] === second[0].toUpperCase() &&
    second.slice(1) === second.slice(1).toLowerCase()
  ) {
    second = second.toLowerCase();
  }
  return `${first}${second}`;
}

function collapseBrokenWordFragments(text: string): string {
  const tripleRegex = /\b([A-Za-z]{4,})\s+(and|or)\s+([A-Za-z]{2,6})\b/gi;
  text = text.replace(tripleRegex, (match, first, middle, last) => {
    if (!MIDDLE_JOINERS.has(String(middle).toLowerCase()) || !SUFFIXES.has(String(last).toLowerCase()) || String(first).length < 5) {
      return match;
    }
    let tail = `${middle}${last}`;
    if (String(first)[0] === String(first)[0].toUpperCase() && String(first).slice(1) === String(first).slice(1).toLowerCase()) {
      tail = tail.toLowerCase();
    }
    return `${first}${tail}`;
  });

  const pairRegex = /\b([A-Za-z]{3,})\s+([A-Za-z]{2,6})\b/g;
  text = text.replace(pairRegex, (match, first, second) => {
    if (!SUFFIXES.has(String(second).toLowerCase()) || String(first).length < 3) {
      return match;
    }
    return mergeCase(String(first), String(second));
  });

  return text;
}

function applyKnownFixes(text: string): string {
  return text
    .replace(/\bresp\s*on\s*ses\b/gi, "responses")
    .replace(/\bresp\s*on\s*ders\b/gi, "responders")
    .replace(/\benvir\s*on\s*ments\b/gi, "environments")
    .replace(/\bpers\s*on\s*alised\b/gi, "personalised")
    .replace(/\bpers\s*on\s*alized\b/gi, "personalized");
}

export default function normalizePdfText(value: string): string {
  if (!value) {
    return "";
  }

  let text = String(value).replace(/\u00a0/g, " ");
  text = text.replace(INVISIBLE_PDF_CHARS, "");
  text = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  text = text.replace(HYPHEN_LINE_BREAK, "$1-$2");
  text = text.replace(/\s*\n+\s*/g, " ");
  text = collapseBrokenWordFragments(text);
  text = applyKnownFixes(text);
  return text.replace(/\s+/g, " ").trim();
}

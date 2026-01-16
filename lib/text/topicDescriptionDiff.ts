import { normalizeTopicText } from "./normalizeTopicText";
import { topicDescriptionBlocks, InlineToken, Block } from "./topicDescriptionBlocks";

export type TopicDescriptionDiffPair = {
  kind: "paragraph" | "bullet";
  pdfText: string;
  portalText: string;
  similarity: number;
};

export type TopicDescriptionDiffResult = {
  alignmentScore: number;
  alignedPairs: TopicDescriptionDiffPair[];
  missingInPortal: string[];
  missingInPdf: string[];
  notableDifferences: string[];
  sectionSummary: {
    missingInPortal: string[];
    missingInPdf: string[];
  };
  counts: {
    pdfParagraphs: number;
    pdfBullets: number;
    portalParagraphs: number;
    portalBullets: number;
    matchedParagraphs: number;
    matchedBullets: number;
  };
};

const sectionLabels = [
  "Expected Outcome",
  "Scope",
  "Applicants",
  "Eligibility",
  "Expected Impact",
  "Budget",
  "Objectives",
  "Activities",
  "Type of Action",
  "Specific Challenge",
];

const maxNotableDifferences = 5;
const highSimilarityThreshold = 0.78;

function normalizeForDiff(input: string): string {
  return normalizeTopicText(input)
    .replace(/[“”]/g, "\"")
    .replace(/[‘’]/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

function tokensToText(tokens: InlineToken[]): string {
  return tokens
    .filter((token) => token.kind !== "ref")
    .map((token) => (token.kind === "label" ? token.text : token.text))
    .join("")
    .trim();
}

function extractItems(blocks: Block[]): Array<{ kind: "paragraph" | "bullet"; text: string }> {
  const items: Array<{ kind: "paragraph" | "bullet"; text: string }> = [];
  blocks.forEach((block) => {
    if (block.kind === "paragraph") {
      const text = tokensToText(block.tokens).trim();
      if (text) {
        items.push({ kind: "paragraph", text });
      }
      return;
    }
    if (block.kind === "list") {
      block.items.forEach((item) => {
        const text = tokensToText(item).trim();
        if (text) {
          items.push({ kind: "bullet", text });
        }
      });
    }
  });
  return items;
}

function extractLabels(text: string): string[] {
  const matches: string[] = [];
  sectionLabels.forEach((label) => {
    const regex = new RegExp(`^${label}\\b`, "i");
    if (regex.test(text.trim())) {
      matches.push(label);
    }
  });
  return matches;
}

function listSectionLabels(items: Array<{ kind: "paragraph" | "bullet"; text: string }>): string[] {
  const labels = new Set<string>();
  items.forEach((item) => {
    extractLabels(item.text).forEach((label) => labels.add(label));
  });
  return [...labels];
}

function tokenSet(input: string): Set<string> {
  const matches = input.toLowerCase().match(/[\p{L}\p{N}]+/gu) ?? [];
  return new Set(matches);
}

function jaccardSimilarity(a: string, b: string): number {
  const setA = tokenSet(a);
  const setB = tokenSet(b);
  if (!setA.size && !setB.size) {
    return 1;
  }
  if (!setA.size || !setB.size) {
    return 0;
  }
  let intersection = 0;
  setA.forEach((token) => {
    if (setB.has(token)) {
      intersection += 1;
    }
  });
  const union = setA.size + setB.size - intersection;
  return union === 0 ? 0 : intersection / union;
}

function summarizeSnippet(text: string, limit = 160): string {
  const normalized = text.replace(/\s+/g, " ").trim();
  if (normalized.length <= limit) {
    return normalized;
  }
  return `${normalized.slice(0, limit - 1)}…`;
}

function alignItems(
  pdfItems: Array<{ kind: "paragraph" | "bullet"; text: string }>,
  portalItems: Array<{ kind: "paragraph" | "bullet"; text: string }>,
): {
  alignedPairs: TopicDescriptionDiffPair[];
  missingInPortal: string[];
  missingInPdf: string[];
  matchedParagraphs: number;
  matchedBullets: number;
} {
  const usedPortalIndexes = new Set<number>();
  const alignedPairs: TopicDescriptionDiffPair[] = [];
  const missingInPortal: string[] = [];

  let matchedParagraphs = 0;
  let matchedBullets = 0;

  pdfItems.forEach((pdfItem) => {
    let bestIndex = -1;
    let bestScore = 0;
    portalItems.forEach((portalItem, portalIndex) => {
      if (usedPortalIndexes.has(portalIndex)) {
        return;
      }
      if (portalItem.kind !== pdfItem.kind) {
        return;
      }
      const similarity = jaccardSimilarity(
        normalizeForDiff(pdfItem.text),
        normalizeForDiff(portalItem.text),
      );
      if (similarity > bestScore) {
        bestScore = similarity;
        bestIndex = portalIndex;
      }
    });

    if (bestIndex >= 0 && bestScore >= highSimilarityThreshold) {
      const portalMatch = portalItems[bestIndex];
      usedPortalIndexes.add(bestIndex);
      alignedPairs.push({
        kind: pdfItem.kind,
        pdfText: pdfItem.text,
        portalText: portalMatch.text,
        similarity: Number(bestScore.toFixed(2)),
      });
      if (pdfItem.kind === "paragraph") {
        matchedParagraphs += 1;
      } else {
        matchedBullets += 1;
      }
    } else {
      missingInPortal.push(pdfItem.text);
    }
  });

  const missingInPdf: string[] = [];
  portalItems.forEach((portalItem, portalIndex) => {
    if (usedPortalIndexes.has(portalIndex)) {
      return;
    }
    missingInPdf.push(portalItem.text);
  });

  return {
    alignedPairs,
    missingInPortal,
    missingInPdf,
    matchedParagraphs,
    matchedBullets,
  };
}

export function buildTopicDescriptionDiff(
  pdfText: string,
  portalText: string,
): TopicDescriptionDiffResult {
  const pdfBlocks = topicDescriptionBlocks(pdfText);
  const portalBlocks = topicDescriptionBlocks(portalText);
  const pdfItems = extractItems(pdfBlocks);
  const portalItems = extractItems(portalBlocks);
  const pdfParagraphs = pdfItems.filter((item) => item.kind === "paragraph");
  const pdfBullets = pdfItems.filter((item) => item.kind === "bullet");
  const portalParagraphs = portalItems.filter((item) => item.kind === "paragraph");
  const portalBullets = portalItems.filter((item) => item.kind === "bullet");

  const alignment = alignItems(pdfItems, portalItems);

  const pdfSectionLabels = listSectionLabels(pdfItems);
  const portalSectionLabels = listSectionLabels(portalItems);
  const missingSectionsInPortal = pdfSectionLabels.filter(
    (label) => !portalSectionLabels.includes(label),
  );
  const missingSectionsInPdf = portalSectionLabels.filter(
    (label) => !pdfSectionLabels.includes(label),
  );

  const paragraphMatchRate = pdfParagraphs.length
    ? alignment.matchedParagraphs / pdfParagraphs.length
    : 1;
  const bulletMatchRate = pdfBullets.length
    ? alignment.matchedBullets / pdfBullets.length
    : 1;

  // Alignment score is display-only; it does not affect parsing or exports.
  let alignmentScore = paragraphMatchRate * 50 + bulletMatchRate * 50;
  alignmentScore -= missingSectionsInPortal.length * 4;
  alignmentScore -= missingSectionsInPdf.length * 4;
  alignmentScore -= (alignment.missingInPortal.length + alignment.missingInPdf.length) * 1.5;
  alignmentScore = Math.max(0, Math.min(100, Math.round(alignmentScore)));

  const notableDifferences: string[] = [];
  alignment.alignedPairs
    .filter((pair) => pair.similarity < 0.9)
    .slice(0, maxNotableDifferences)
    .forEach((pair) => {
      notableDifferences.push(
        `Low similarity (${pair.similarity}) — ${summarizeSnippet(pair.pdfText)}`,
      );
    });

  alignment.missingInPortal
    .slice(0, maxNotableDifferences - notableDifferences.length)
    .forEach((item) => {
      notableDifferences.push(
        `Missing in portal — ${summarizeSnippet(item)}`,
      );
    });

  alignment.missingInPdf
    .slice(0, maxNotableDifferences - notableDifferences.length)
    .forEach((item) => {
      notableDifferences.push(
        `Missing in PDF — ${summarizeSnippet(item)}`,
      );
    });

  return {
    alignmentScore,
    alignedPairs: alignment.alignedPairs,
    missingInPortal: alignment.missingInPortal,
    missingInPdf: alignment.missingInPdf,
    notableDifferences,
    sectionSummary: {
      missingInPortal: missingSectionsInPortal,
      missingInPdf: missingSectionsInPdf,
    },
    counts: {
      pdfParagraphs: pdfParagraphs.length,
      pdfBullets: pdfBullets.length,
      portalParagraphs: portalParagraphs.length,
      portalBullets: portalBullets.length,
      matchedParagraphs: alignment.matchedParagraphs,
      matchedBullets: alignment.matchedBullets,
    },
  };
}

"use client";

import React, { useMemo, useState } from "react";

import { TopicDescriptionFormatted } from "./TopicDescriptionFormatted";
import { PortalTopicDescription } from "../lib/portal/extractPortalTopicDescription";
import {
  buildTopicDescriptionDiff,
  TopicDescriptionDiffResult,
} from "../lib/text/topicDescriptionDiff";

// Thresholds are deterministic and used for display-only confidence badges.
const alignmentThresholds = {
  aligned: 90,
  mostly: 75,
};

type TopicDescriptionPanelProps = {
  pdfText: string;
  portalTopic?: PortalTopicDescription | null;
  portalSummary?: string | null;
};

type BadgeTone = "green" | "amber" | "red";

function getAlignmentBadge(diff: TopicDescriptionDiffResult | null): {
  tone: BadgeTone;
  label: string;
} | null {
  if (!diff) {
    return null;
  }
  const missingItems = diff.missingInPortal.length + diff.missingInPdf.length;
  const missingSections =
    diff.sectionSummary.missingInPortal.length +
    diff.sectionSummary.missingInPdf.length;
  if (
    diff.alignmentScore >= alignmentThresholds.aligned &&
    diff.missingInPortal.length <= 1 &&
    diff.missingInPdf.length <= 1 &&
    missingSections <= 1
  ) {
    return { tone: "green", label: "Aligned with official portal" };
  }
  if (diff.alignmentScore >= alignmentThresholds.mostly || missingItems <= 3) {
    return { tone: "amber", label: "Mostly aligned" };
  }
  return { tone: "red", label: "Mismatch" };
}

function badgeClassName(tone: BadgeTone): string {
  if (tone === "green") {
    return "bg-emerald-50 text-emerald-700 border-emerald-200";
  }
  if (tone === "amber") {
    return "bg-amber-50 text-amber-700 border-amber-200";
  }
  return "bg-rose-50 text-rose-700 border-rose-200";
}

export function TopicDescriptionPanel({
  pdfText,
  portalTopic,
  portalSummary,
}: TopicDescriptionPanelProps) {
  const [usePortal, setUsePortal] = useState(false);
  const [useAiSummary, setUseAiSummary] = useState(false);
  const [showDiff, setShowDiff] = useState(false);

  const portalAvailable = Boolean(portalTopic?.rawText);
  const displayText = usePortal && portalAvailable
    ? useAiSummary && portalSummary
      ? portalSummary
      : portalTopic?.rawText ?? ""
    : pdfText;

  const diffResult = useMemo(() => {
    if (!usePortal || !portalAvailable) {
      return null;
    }
    return buildTopicDescriptionDiff(pdfText, portalTopic?.rawText ?? "");
  }, [usePortal, portalAvailable, pdfText, portalTopic?.rawText]);

  const badge = getAlignmentBadge(diffResult);

  return (
    <section className="bg-[#f6f4ef] py-8 px-4 sm:px-6">
      <div className="mx-auto max-w-[1150px] rounded-md border border-slate-200 bg-white shadow-sm">
        <div className="border-b border-slate-200 px-6 py-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex flex-wrap items-center gap-3">
            <span className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">
              Topic Description
            </span>
            {badge ? (
              <span
                className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold ${badgeClassName(
                  badge.tone,
                )}`}
              >
                {badge.label}
              </span>
            ) : null}
            {usePortal && portalAvailable ? (
              <span className="text-xs text-slate-500">
                Source: Funding &amp; Tenders Portal
              </span>
            ) : null}
            {usePortal && portalAvailable ? (
              <button
                type="button"
                className="text-xs font-semibold text-blue-700 hover:text-blue-800 hover:underline underline-offset-2"
                onClick={() => setShowDiff((prev) => !prev)}
              >
                {showDiff ? "Hide differences" : "View differences"}
              </button>
            ) : null}
          </div>
          <div className="flex flex-wrap items-center gap-4 text-xs text-slate-600">
            <label className="inline-flex items-center gap-2">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border-slate-300 text-blue-600"
                checked={usePortal}
                disabled={!portalAvailable}
                onChange={(event) => setUsePortal(event.target.checked)}
              />
              <span>Use official Topic page (EU Portal)</span>
            </label>
            {portalAvailable ? (
              <label className="inline-flex items-center gap-2">
                <input
                  type="checkbox"
                  className="h-4 w-4 rounded border-slate-300 text-blue-600"
                  checked={useAiSummary}
                  disabled={!portalSummary || !usePortal}
                  onChange={(event) => setUseAiSummary(event.target.checked)}
                />
                <span>Use AI readability summary</span>
              </label>
            ) : null}
            {portalAvailable ? (
              <label className="inline-flex items-center gap-2">
                <input
                  type="checkbox"
                  className="h-4 w-4 rounded border-slate-300 text-blue-600"
                  checked={showDiff}
                  disabled={!usePortal}
                  onChange={(event) => setShowDiff(event.target.checked)}
                />
                <span>Show PDF vs Portal diff</span>
              </label>
            ) : null}
          </div>
        </div>
        <div className="px-6 py-5">
          <TopicDescriptionFormatted text={displayText} />
          {showDiff && diffResult ? (
            <div className="mt-4 rounded-md border border-slate-200 bg-slate-50 px-4 py-3 text-xs text-slate-700">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <span className="font-semibold text-slate-700">
                  PDF vs Portal comparison
                </span>
                <span className="text-slate-600">
                  Alignment score: {diffResult.alignmentScore}/100
                </span>
              </div>
              <div className="mt-2 grid gap-2 sm:grid-cols-2">
                <div>
                  <div className="font-semibold text-slate-600">Matched</div>
                  <div>
                    {diffResult.counts.matchedParagraphs}/
                    {diffResult.counts.pdfParagraphs} paragraphs, {" "}
                    {diffResult.counts.matchedBullets}/
                    {diffResult.counts.pdfBullets} bullets
                  </div>
                </div>
                <div>
                  <div className="font-semibold text-slate-600">Missing</div>
                  <div>
                    {diffResult.missingInPortal.length} missing in portal, {" "}
                    {diffResult.missingInPdf.length} missing in PDF
                  </div>
                </div>
              </div>
              {diffResult.sectionSummary.missingInPortal.length ||
              diffResult.sectionSummary.missingInPdf.length ? (
                <div className="mt-2">
                  <div className="font-semibold text-slate-600">Sections</div>
                  <div className="flex flex-wrap gap-2">
                    {diffResult.sectionSummary.missingInPortal.length ? (
                      <span>
                        Missing in portal: {" "}
                        {diffResult.sectionSummary.missingInPortal.join(", ")}
                      </span>
                    ) : null}
                    {diffResult.sectionSummary.missingInPdf.length ? (
                      <span>
                        Missing in PDF: {" "}
                        {diffResult.sectionSummary.missingInPdf.join(", ")}
                      </span>
                    ) : null}
                  </div>
                </div>
              ) : null}
              {diffResult.notableDifferences.length ? (
                <div className="mt-3">
                  <div className="font-semibold text-slate-600">
                    Notable differences
                  </div>
                  <ul className="mt-1 list-disc space-y-1 pl-4">
                    {diffResult.notableDifferences.map((diff, index) => (
                      <li key={`diff-${index}`}>{diff}</li>
                    ))}
                  </ul>
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>
    </section>
  );
}

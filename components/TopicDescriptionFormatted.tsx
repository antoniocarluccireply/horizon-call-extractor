import React from "react";

import { topicDescriptionBlocks } from "../lib/text/topicDescriptionBlocks";

type TopicDescriptionFormattedProps = {
  text: string;
  className?: string;
};

export function TopicDescriptionFormatted({
  text,
  className,
}: TopicDescriptionFormattedProps) {
  const blocks = topicDescriptionBlocks(text);
  const getReferenceLabel = (url?: string) => {
    if (!url) {
      return null;
    }
    return url.replace(/^https?:\/\//, "").split(/[/?#]/)[0];
  };

  if (!blocks.length) {
    return null;
  }

  return (
    <div
      className={[
        "topic-description text-sm leading-[1.45] text-slate-800",
        className ?? "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      {blocks.map((block, blockIndex) => {
        if (block.kind === "paragraph") {
          return (
            <p key={`paragraph-${blockIndex}`} className="mb-2">
              {block.tokens.flatMap((token, tokenIndex) => {
                if (token.kind === "text") {
                  return token.text;
                }
                if (token.kind === "label") {
                  return (
                    <strong key={`p-${blockIndex}-label-${tokenIndex}`}>
                      {token.text}
                    </strong>
                  );
                }
                return (
                  <sup
                    key={`p-${blockIndex}-ref-${tokenIndex}`}
                    className="ml-0.5 align-super text-[0.7rem] text-slate-600"
                  >
                    <a
                      href={`#reference-${token.index}`}
                      className="text-blue-700 hover:text-blue-800 hover:underline underline-offset-2"
                    >
                      [{token.index}]
                    </a>
                  </sup>
                );
              })}
            </p>
          );
        }
        if (block.kind === "list") {
          return (
            <ul
              key={`list-${blockIndex}`}
              className="list-disc list-outside pl-5 space-y-1 mt-1 mb-2"
            >
              {block.items.map((item, itemIndex) => (
                <li key={`list-${blockIndex}-item-${itemIndex}`} className="pl-1">
                  {item.flatMap((token, tokenIndex) => {
                    if (token.kind === "text") {
                      return token.text;
                    }
                    if (token.kind === "label") {
                      return (
                        <strong key={`l-${blockIndex}-${itemIndex}-label-${tokenIndex}`}>
                          {token.text}
                        </strong>
                      );
                    }
                    return (
                      <sup
                        key={`l-${blockIndex}-${itemIndex}-ref-${tokenIndex}`}
                        className="ml-0.5 align-super text-[0.7rem] text-slate-600"
                      >
                        <a
                          href={`#reference-${token.index}`}
                          className="text-blue-700 hover:text-blue-800 hover:underline underline-offset-2"
                        >
                          [{token.index}]
                        </a>
                      </sup>
                    );
                  })}
                </li>
              ))}
            </ul>
          );
        }
        if (block.kind === "references") {
          return (
            <div key={`references-${blockIndex}`} className="mt-4">
              <div className="text-xs font-semibold text-slate-600 mb-1">
                References
              </div>
              <ul className="text-xs leading-snug text-slate-600 space-y-1">
                {block.items.map((item) => {
                  const label =
                    item.text ??
                    (item.url ? getReferenceLabel(item.url) : undefined);
                  return (
                    <li
                      key={`ref-${item.index}`}
                      className="flex gap-2 items-start"
                      id={`reference-${item.index}`}
                    >
                      <span>[{item.index}]</span>
                      <span className="flex flex-wrap gap-1">
                        {label ? <span>{label}</span> : null}
                        {item.url ? (
                          <>
                            {label ? <span aria-hidden="true">—</span> : null}
                            <a
                              className="text-blue-700 break-all hover:text-blue-800 hover:underline underline-offset-2"
                              href={item.url}
                              target="_blank"
                              rel="noreferrer"
                            >
                              {item.url}
                            </a>
                          </>
                        ) : (
                          <span>Reference unavailable</span>
                        )}
                      </span>
                    </li>
                  );
                })}
              </ul>
            </div>
          );
        }
        return null;
      })}
    </div>
  );
}

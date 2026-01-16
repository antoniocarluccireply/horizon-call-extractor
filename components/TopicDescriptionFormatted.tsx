import React from "react";

import { topicDescriptionBlocks } from "../lib/text/topicDescriptionBlocks";

type TopicDescriptionFormattedProps = {
  text: string;
};

export function TopicDescriptionFormatted({
  text,
}: TopicDescriptionFormattedProps) {
  const blocks = topicDescriptionBlocks(text);

  if (!blocks.length) {
    return null;
  }

  return (
    <>
      {blocks.map((block, blockIndex) => {
        if (block.kind === "paragraph") {
          return (
            <p key={`paragraph-${blockIndex}`} className="text-sm leading-snug mb-2">
              {block.tokens.flatMap((token, tokenIndex) => {
                if (token.kind === "text") {
                  return token.text;
                }
                return (
                  <sup
                    key={`p-${blockIndex}-ref-${tokenIndex}`}
                    className="ml-0.5 align-super text-xs text-slate-600"
                  >
                    [{token.index}]
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
              className="list-disc list-outside pl-6 space-y-1 mt-0 mb-3 text-sm leading-snug"
            >
              {block.items.map((item, itemIndex) => (
                <li key={`list-${blockIndex}-item-${itemIndex}`}>
                  {item.flatMap((token, tokenIndex) => {
                    if (token.kind === "text") {
                      return token.text;
                    }
                    return (
                      <sup
                        key={`l-${blockIndex}-${itemIndex}-ref-${tokenIndex}`}
                        className="ml-0.5 align-super text-xs text-slate-600"
                      >
                        [{token.index}]
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
                {block.items.map((item) => (
                  <li
                    key={`ref-${item.index}`}
                    className="flex gap-2 items-start"
                  >
                    <span>[{item.index}]</span>
                    <span className="flex flex-col gap-0.5">
                      {item.text ? <span>{item.text}</span> : null}
                      {item.url ? (
                        <a
                          className="text-blue-700 underline break-all visited:text-purple-700"
                          href={item.url}
                          target="_blank"
                          rel="noreferrer"
                        >
                          {item.url}
                        </a>
                      ) : (
                        <span>Reference unavailable</span>
                      )}
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          );
        }
        return null;
      })}
    </>
  );
}

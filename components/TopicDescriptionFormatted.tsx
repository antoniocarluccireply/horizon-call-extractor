import React from "react";

import { topicDescriptionBlocks } from "../lib/text/topicDescriptionBlocks";

type TopicDescriptionFormattedProps = {
  text: string;
};

function renderTextWithLinks(text: string, keyPrefix: string) {
  const urlRegex = /https?:\/\/[^\s<]+/g;
  const nodes: React.ReactNode[] = [];
  let lastIndex = 0;
  let match: RegExpExecArray | null;
  let linkIndex = 0;

  while ((match = urlRegex.exec(text)) !== null) {
    if (match.index > lastIndex) {
      nodes.push(text.slice(lastIndex, match.index));
    }
    let url = match[0];
    let trailing = "";
    while (/[),.;:]+$/.test(url)) {
      trailing = url.slice(-1) + trailing;
      url = url.slice(0, -1);
    }
    if (url) {
      nodes.push(
        <a
          key={`${keyPrefix}-link-${linkIndex}`}
          className="text-blue-700 underline break-all visited:text-purple-700"
          href={url}
          target="_blank"
          rel="noreferrer"
        >
          {url}
        </a>,
      );
      linkIndex += 1;
    }
    if (trailing) {
      nodes.push(trailing);
    }
    lastIndex = match.index + match[0].length;
  }

  if (lastIndex < text.length) {
    nodes.push(text.slice(lastIndex));
  }

  return nodes;
}

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
                  return renderTextWithLinks(
                    token.text,
                    `p-${blockIndex}-t-${tokenIndex}`,
                  );
                }
                return (
                  <sup
                    key={`p-${blockIndex}-ref-${tokenIndex}`}
                    className="ml-0.5 text-xs text-slate-600"
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
                      return renderTextWithLinks(
                        token.text,
                        `l-${blockIndex}-${itemIndex}-t-${tokenIndex}`,
                      );
                    }
                    return (
                      <sup
                        key={`l-${blockIndex}-${itemIndex}-ref-${tokenIndex}`}
                        className="ml-0.5 text-xs text-slate-600"
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

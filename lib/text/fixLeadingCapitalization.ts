export function fixLeadingCapitalization(title: string): string {
  if (!title) {
    return "";
  }
  const trimmed = title.trimStart();
  if (!trimmed) {
    return "";
  }
  const firstChar = trimmed[0];
  if (!firstChar.match(/[A-Za-z]/)) {
    return trimmed;
  }
  if (firstChar === firstChar.toUpperCase()) {
    return trimmed;
  }
  return firstChar.toUpperCase() + trimmed.slice(1);
}

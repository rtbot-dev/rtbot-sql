import { Message } from "./types";

export interface FormattedRow {
  timestamp: number;
  [field: string]: number;
}

export function formatRow(
  msg: Message,
  fieldMap: Record<string, number>,
): FormattedRow {
  const row: FormattedRow = { timestamp: msg.timestamp };
  for (const [name, idx] of Object.entries(fieldMap)) {
    row[name] = msg.values[idx] ?? 0;
  }
  return row;
}

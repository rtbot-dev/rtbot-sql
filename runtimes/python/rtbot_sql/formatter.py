"""Helpers to format runtime outputs."""

from __future__ import annotations

from typing import Dict, Iterable, List, Sequence

try:
  import pandas as pd
  _HAS_PANDAS = True
except Exception:  # pragma: no cover - optional dependency
  pd = None
  _HAS_PANDAS = False


def ordered_columns(field_map: Dict[str, int]) -> List[str]:
  return [name for name, _ in sorted(field_map.items(), key=lambda item: item[1])]


def project_rows(rows: Iterable[Sequence[float]], field_map: Dict[str, int]) -> List[List[float]]:
  ordered = sorted(field_map.items(), key=lambda item: item[1])
  indices = [index for _, index in ordered]

  projected: List[List[float]] = []
  for row in rows:
    projected.append([
        float(row[idx]) if 0 <= idx < len(row) else 0.0
        for idx in indices
    ])
  return projected


def format_rows(
    rows: Iterable[Sequence[float]],
    field_map: Dict[str, int],
    as_dataframe: bool = True,
):
  columns = ordered_columns(field_map)
  normalized = project_rows(rows, field_map)

  if as_dataframe and _HAS_PANDAS:
    return pd.DataFrame(normalized, columns=columns)

  return {
      "columns": columns,
      "rows": normalized,
  }

"""Helpers to format runtime outputs."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence

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
    timestamps: Optional[List[int]] = None,
    time_values: Optional[List[Any]] = None,
    time_column: str = "time",
    as_dataframe: bool = True,
):
  columns = ordered_columns(field_map)
  normalized = project_rows(rows, field_map)

  if as_dataframe and _HAS_PANDAS:
    df = pd.DataFrame(normalized, columns=columns)
    if timestamps is not None:
      values = time_values if time_values is not None else timestamps
      df.insert(0, time_column, values)
    # Auto-normalize: rename _time -> time and convert numeric timestamps.
    if '_time' in df.columns and 'time' not in df.columns:
      df = df.rename(columns={'_time': 'time'})
      if pd.api.types.is_numeric_dtype(df['time']):
        df['time'] = pd.to_datetime(df['time'], unit='ms')
    return df

  result = {
      "columns": columns,
      "rows": normalized,
  }
  if timestamps is not None:
    result["timestamps"] = timestamps
    result["time_column"] = time_column
  return result


def normalize_time_column(df, default_unit='ms'):
  if 'time' in df.columns:
    return df
  if '_time' in df.columns:
    out = df.rename(columns={'_time': 'time'}).copy()
    if pd.api.types.is_numeric_dtype(out['time']):
      out['time'] = pd.to_datetime(out['time'], unit=default_unit)
    return out
  raise KeyError("Expected a time column in SQL result ('time' or '_time')")

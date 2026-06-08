"""User-facing Python runtime API for rtbot-sql."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .catalog import InMemoryCatalog, StreamSchema, ViewMeta
from .compiler import compile_select_to_program, compile_sql, native
from .formatter import format_rows
from .jupyter import show_graph as display_graph
from .pipeline_runner import LocalPipelineRunner
from .stream_store import InMemoryStreamStore

try:
  import numpy as np
  _HAS_NUMPY = True
except ImportError:
  _HAS_NUMPY = False


class SqlError(RuntimeError):
  def __init__(self, errors: Sequence[Any]):
    self.errors = list(errors)
    messages = [getattr(err, "message", str(err)) for err in self.errors]
    super().__init__("; ".join(messages) if messages else "SQL error")


@dataclass
class InsertResult:
  rows: int
  elapsed_s: float
  rows_per_sec: float

  def __repr__(self):
    return f"Inserted {self.rows:,} rows in {self.elapsed_s:.2f}s ({self.rows_per_sec:,.0f} rows/sec)"


def _enum_name(value: Any) -> str:
  if hasattr(value, "name"):
    return str(value.name)
  text = str(value)
  if "." in text:
    return text.split(".")[-1]
  return text


def _extract_limit(sql: str) -> Optional[int]:
  match = re.search(r"\bLIMIT\s+(\d+)\b", sql, flags=re.IGNORECASE)
  if not match:
    return None
  return int(match.group(1))


class RtBotSql:
  def __init__(self) -> None:
    self._catalog = InMemoryCatalog()
    self._store = InMemoryStreamStore()
    self._runner = LocalPipelineRunner()

    self._last_timestamp = int(time.time() * 1000)
    self._time_column_name = "time"
    self._time_unit_override: Optional[str] = None
    self._time_formatter: Optional[Callable[[List[int]], List[Any]]] = None
    self._ts_units_per_second: int = 1_000_000

    # Consolidated-session state. Every registered view is compiled
    # into a single rtbot Program at CREATE VIEW / first insert time.
    # DDL invalidates the session so the next deploy picks up the
    # updated catalog; `_backfill_session_from_store` replays stored
    # source data so newly registered views inherit history.
    self._session_pipeline: Optional[Any] = None
    self._session_op_to_view: Dict[str, str] = {}
    self._session_stream_port: Dict[str, str] = {}

  def _next_timestamp(self) -> int:
    now = int(time.time() * 1000)
    if now <= self._last_timestamp:
      self._last_timestamp += 1
    else:
      self._last_timestamp = now
    return self._last_timestamp

  @staticmethod
  def _normalize_sql(sql: str) -> str:
    out = sql.strip()
    out = re.sub(r"^\s*CREATE\s+STREAM\b", "CREATE TABLE", out, flags=re.IGNORECASE)
    out = re.sub(r"^\s*DROP\s+STREAM\b", "DROP TABLE", out, flags=re.IGNORECASE)
    return out

  def execute(self, sql: str):
    compilation = compile_sql(sql, self._catalog.snapshot(), self._ts_units_per_second)

    new_ts = compilation["new_ts_units_per_second"]
    if new_ts > 0:
      self._ts_units_per_second = new_ts
      return None

    last_select_result = None
    for result in compilation["results"]:
      if result.has_errors():
        raise SqlError(result.errors)

      statement = result.statement_type
      if statement == native.StatementType.CREATE_STREAM:
        self._handle_create_stream(result)

      elif statement == native.StatementType.INSERT:
        self._handle_insert(result)

      elif statement in (
          native.StatementType.CREATE_VIEW,
          native.StatementType.CREATE_MATERIALIZED_VIEW,
      ):
        self._handle_create_view(result, statement)

      elif statement == native.StatementType.DROP:
        self._handle_drop(result)

      elif statement == native.StatementType.SELECT:
        last_select_result = self._handle_select(sql, result)

      else:
        raise SqlError([f"Unsupported statement type: {statement}"])

    return last_select_result

  def _handle_create_stream(self, result: native.CompilationResult) -> None:
    self._catalog.register_stream(result.entity_name, result.stream_schema)

  def _handle_insert(self, result: native.CompilationResult) -> None:
    stream_name = result.entity_name
    schema = self._catalog.lookup_stream(stream_name)
    table = None
    expected_cols = -1
    if schema is not None:
      expected_cols = len(schema.columns) if schema.columns else -1
    else:
      table = self._catalog.lookup_table(stream_name)
      if table is None:
        raise SqlError([f"Unknown stream or table: {stream_name}"])
      expected_cols = len(table.columns) if table.columns else -1

    payload = [float(v) for v in result.insert_payload]
    if expected_cols > 0 and len(payload) != expected_cols:
      raise SqlError([
          f"INSERT payload length mismatch for {stream_name}: "
          f"expected {expected_cols}, got {len(payload)}"
      ])

    # Sync dictionary updates from C++ compiler back to Python catalog.
    if hasattr(result, 'dictionary_updates') and result.dictionary_updates:
      for dict_key, entries in result.dictionary_updates.items():
        d = self._catalog.get_or_create_dictionary(dict_key)
        for fid, s in entries.items():
          d.put_mapping(float(fid), s)

    ts = self._next_timestamp()
    self._ensure_session_deployed()
    self._append_and_propagate(stream_name, ts, payload)
    self._feed_session_row(stream_name, ts, payload)

  def insert_mixed(self, stream_name: str, timestamp: int, values: list) -> None:
    """Insert a row with mixed types (doubles and strings for TEXT columns).

    Strings are dictionary-encoded to doubles before passing to the
    append pipeline. Each TEXT column gets its own StringDictionary
    keyed by "streamName.columnName".
    """
    if stream_name is None:
      raise ValueError("stream_name must not be None")
    schema = self._catalog.lookup_stream(stream_name)
    if schema is None:
      raise ValueError(f"unknown stream: {stream_name}")
    if len(values) != len(schema.columns):
      raise ValueError(
          f"expected {len(schema.columns)} values, got {len(values)}")

    encoded = []
    for i, col in enumerate(schema.columns):
      val = values[i]
      if col.is_text:
        if not isinstance(val, str):
          raise ValueError(
              f"expected str for TEXT column '{col.name}', got {type(val).__name__}")
        dict_key = f"{stream_name}.{col.name}"
        d = self._catalog.get_or_create_dictionary(dict_key)
        encoded.append(d.encode(val))
      else:
        if isinstance(val, str):
          raise ValueError(
              f"expected number for DOUBLE column '{col.name}', got str")
        encoded.append(float(val))

    self._ensure_session_deployed()
    self._append_and_propagate(stream_name, timestamp, encoded)
    self._feed_session_row(stream_name, timestamp, encoded)

  def decode_row(self, stream_name: str, values: list) -> list:
    """Decode a row of double values, converting TEXT column IDs back to strings.

    For each TEXT column, looks up the double value in the corresponding
    StringDictionary. Returns the original double if the stream is unknown
    or no dictionary mapping exists.
    """
    schema = self._catalog.lookup_stream(stream_name)
    if schema is None or values is None:
      return list(values) if values else []

    decoded = []
    for i, val in enumerate(values):
      if i < len(schema.columns) and schema.columns[i].is_text:
        dict_key = f"{stream_name}.{schema.columns[i].name}"
        d = self._catalog.lookup_dictionary(dict_key)
        if d is not None:
          s = d.decode(float(val))
          decoded.append(s if s is not None else val)
        else:
          decoded.append(val)
      else:
        decoded.append(val)
    return decoded

  def _handle_create_view(
      self,
      result: native.CompilationResult,
      statement: Any,
  ) -> None:
    name = result.entity_name
    materialized = statement == native.StatementType.CREATE_MATERIALIZED_VIEW

    view_meta = ViewMeta(
        name=name,
        entity_type=(
            native.EntityType.MATERIALIZED_VIEW
            if materialized
            else native.EntityType.VIEW
        ),
        view_type=result.view_type,
        field_map=dict(result.field_map),
        source_streams=list(result.source_streams),
        program_json=result.program_json,
        output_stream=name,
        per_key_prefix=f"{name}:key:",
        known_keys=[],
        key_index=int(result.key_index),
    )

    self._catalog.register_view(name, view_meta)
    # Invalidate and eagerly redeploy the consolidated session so the
    # new view immediately participates (and picks up already-stored
    # source-stream data via `_backfill_session_from_store`).
    self._invalidate_session()
    self._ensure_session_deployed()

  def _handle_drop(self, result: native.CompilationResult) -> None:
    name = result.drop_entity_name
    self._catalog.drop(name)
    self._store.clear(name)
    self._invalidate_session()

  def _append_and_propagate(
      self,
      stream_name: str,
      timestamp: int,
      values: List[float],
  ) -> None:
    """Per-message side effects for an entity (stream / table / view).

    Stores the row and updates keyed-view key tracking. Dependent-view
    propagation happens inside the consolidated session Program, not
    here.
    """
    view = self._catalog.lookup_view(stream_name)
    is_plain_view = (
        view is not None
        and view.entity_type == native.EntityType.VIEW
    )

    if not is_plain_view:
      self._store.append(stream_name, timestamp, values)
      if (
          view is not None
          and view.view_type == native.ViewType.KEYED
          and 0 <= view.key_index < len(values)
      ):
        self._catalog.add_key(stream_name, float(values[view.key_index]))

  def _project_messages(
      self,
      messages: Sequence[Any],
      field_map: Dict[str, int],
  ) -> List[List[float]]:
    ordered = sorted(field_map.items(), key=lambda item: item[1])
    indices = [index for _, index in ordered]

    rows: List[List[float]] = []
    for msg in messages:
      values = list(msg.values)
      rows.append([
          float(values[idx]) if 0 <= idx < len(values) else 0.0
          for idx in indices
      ])
    return rows

  def _resolve_field_map(self, source: str, field_map: Dict[str, int]) -> Dict[str, int]:
    if field_map:
      return dict(field_map)

    source_view = self._catalog.lookup_view(source)
    if source_view is not None:
      return dict(source_view.field_map)

    source_stream = self._catalog.lookup_stream(source)
    if source_stream is not None:
      return {column.name: column.index for column in source_stream.columns}

    source_table = self._catalog.lookup_table(source)
    if source_table is not None:
      return {column.name: column.index for column in source_table.columns}

    return {}

  def _format_time_values(self, timestamps: List[int]) -> List[Any]:
    if self._time_formatter is not None:
      return list(self._time_formatter(list(timestamps)))

    if not timestamps:
      return []

    try:
      import pandas as pd  # type: ignore
    except Exception:
      return [int(ts) for ts in timestamps]

    unit = self._time_unit_override
    if unit is None:
      max_abs = max(abs(int(ts)) for ts in timestamps)
      if max_abs >= 10**17:
        unit = "ns"
      elif max_abs >= 10**14:
        unit = "us"
      elif max_abs >= 10**11:
        unit = "ms"
      else:
        unit = "s"

    return list(pd.to_datetime(timestamps, unit=unit))

  def configure_time_format(
      self,
      *,
      unit: Optional[str] = None,
      formatter: Optional[Callable[[List[int]], List[Any]]] = None,
      column_name: str = "time",
  ) -> None:
    if unit is not None and unit not in {"s", "ms", "us", "ns"}:
      raise SqlError([f"Invalid time unit '{unit}'. Expected one of: s, ms, us, ns"])
    self._time_unit_override = unit
    self._time_formatter = formatter
    self._time_column_name = column_name

  def _execute_tier1(self, sql: str, result: native.CompilationResult):
    source = result.source_streams[0] if result.source_streams else ""
    if not source:
      return format_rows([], {}, timestamps=[])

    effective_field_map = self._resolve_field_map(source, dict(result.field_map))
    limit = _extract_limit(sql)
    source_view = self._catalog.lookup_view(source)

    if (
        source_view is not None
        and source_view.view_type == native.ViewType.KEYED
        and " where " not in sql.lower()
    ):
      known_keys = sorted(self._catalog.get_known_keys(source))
      latest_by_key = self._store.read_latest_per_key(
          source,
          known_keys,
          source_view.key_index,
      )
      messages = [latest_by_key[k] for k in known_keys if k in latest_by_key]
      if limit is not None:
        messages = messages[:limit]
    else:
      if limit is None:
        messages = self._store.read(source)
      else:
        messages = self._store.read_latest(source, limit)

    rows = self._project_messages(messages, effective_field_map)
    timestamps = [msg.timestamp for msg in messages]
    return format_rows(
        rows,
        effective_field_map,
        timestamps=timestamps,
        time_values=self._format_time_values(timestamps),
        time_column=self._time_column_name,
    )

  def _execute_with_pipeline(self, sql: str, result: native.CompilationResult):
    original_field_map = dict(result.field_map)
    runtime_result = result
    if not runtime_result.program_json:
      runtime_result = compile_select_to_program(sql, self._catalog.snapshot(), self._ts_units_per_second)
      if runtime_result.has_errors():
        raise SqlError(runtime_result.errors)

    effective_field_map = dict(runtime_result.field_map)
    if not effective_field_map:
      effective_field_map = original_field_map
    if not effective_field_map and runtime_result.source_streams:
      effective_field_map = self._resolve_field_map(runtime_result.source_streams[0], {})

    if not runtime_result.source_streams:
      return format_rows(
          [],
          effective_field_map,
          timestamps=[],
          time_values=[],
          time_column=self._time_column_name,
      )

    # Interleaved replay: collect all source events sorted by (timestamp, source_rank).
    # The compiled RTBot program handles timestamp synchronization internally —
    # operators only combine values sharing the exact same timestamp.
    events: List[Tuple[int, int, str, List[float]]] = []
    for i, source in enumerate(runtime_result.source_streams):
      port = f"i{i + 1}"
      for msg in self._store.read(source):
        events.append((msg.timestamp, i, port, list(msg.values)))
    events.sort(key=lambda item: (item[0], item[1]))
    inputs: List[Tuple[int, List[float], str]] = [
        (ts, vals, port) for ts, _, port, vals in events
    ]

    outputs = self._runner.run_once(runtime_result.program_json, inputs)
    timestamps = [out.timestamp for out in outputs]
    rows = [list(out.values) for out in outputs]

    limit = _extract_limit(sql)
    if limit is not None:
      rows = rows[:limit]
      timestamps = timestamps[:limit]

    return format_rows(
        rows,
        effective_field_map,
        timestamps=timestamps,
        time_values=self._format_time_values(timestamps),
        time_column=self._time_column_name,
    )

  def _handle_select(self, sql: str, result: native.CompilationResult):
    if result.select_tier == native.SelectTier.TIER1_READ:
      return self._execute_tier1(sql, result)
    return self._execute_with_pipeline(sql, result)

  def explain(self, sql: str) -> Dict[str, Any]:
    compilation = compile_sql(sql, self._catalog.snapshot(), self._ts_units_per_second)
    result = compilation["results"][0]

    output = {
        "statement_type": _enum_name(result.statement_type),
        "select_tier": _enum_name(result.select_tier),
        "entity_name": result.entity_name,
        "source_streams": list(result.source_streams),
        "field_map": dict(result.field_map),
        "view_type": _enum_name(result.view_type),
        "key_index": int(result.key_index),
        "program_json": None,
        "errors": [
            {
                "message": err.message,
                "line": int(err.line),
                "column": int(err.column),
            }
            for err in result.errors
        ],
    }

    if result.program_json:
      output["program_json"] = json.loads(result.program_json)

    return output

  def show_graph(self, view_name: str):
    view = self._catalog.lookup_view(view_name)
    if view is None:
      raise SqlError([f"Unknown view: {view_name}"])
    return display_graph(view.program_json)

  def debug(self, sql: str) -> Dict[str, Any]:
    return self.explain(sql)

  def export_for_redis(self, view_name: str) -> str:
    view = self._catalog.lookup_view(view_name)
    if view is None:
      raise SqlError([f"Unknown view: {view_name}"])

    kind = (
        "MATERIALIZED VIEW"
        if view.entity_type == native.EntityType.MATERIALIZED_VIEW
        else "VIEW"
    )
    sources = ", ".join(view.source_streams)
    return (
        "-- Original SQL text is not persisted in the Python runtime catalog.\n"
        f"CREATE {kind} {view_name} AS /* sources: {sources} */;"
    )

  def insert_buffer(
      self,
      stream_name: str,
      timestamps: Any,
      value_columns: Any,
      *,
      store_raw: bool = False,
  ) -> InsertResult:
    """Ingest a batch of rows from numpy arrays without a DataFrame wrapper.

    Parameters are column-major inputs matched to the schema order of the
    target stream:

      - ``timestamps`` — 1D int64-coercible array of monotone timestamps.
      - ``value_columns`` — 2D float64-coercible array of shape
        ``(n_rows, n_cols)`` (row-major); ``n_cols`` must equal the
        target stream's column count.

    Mirrors the Java ``insertBuffer(String, long[], double[][])`` fast
    path: data goes straight to the buffer API (``receive_buffer``) with
    no DataFrame round-trip, no row-by-row Python iteration.
    """
    if not _HAS_NUMPY:
      raise ImportError("insert_buffer requires numpy")

    schema = self._catalog.lookup_stream(stream_name)
    if schema is None:
      raise SqlError([f"Unknown stream: {stream_name}"])

    ts_arr = np.ascontiguousarray(timestamps, dtype=np.int64)
    val_arr = np.ascontiguousarray(value_columns, dtype=np.float64)
    if ts_arr.ndim != 1:
      raise SqlError(["insert_buffer: timestamps must be 1D"])
    if val_arr.ndim != 2:
      raise SqlError(["insert_buffer: value_columns must be 2D"])
    if ts_arr.shape[0] != val_arr.shape[0]:
      raise SqlError([
          "insert_buffer: row count mismatch between timestamps "
          f"({ts_arr.shape[0]}) and value_columns ({val_arr.shape[0]})"
      ])
    if schema.columns and val_arr.shape[1] != len(schema.columns):
      raise SqlError([
          f"insert_buffer column count mismatch for {stream_name}: "
          f"schema={len(schema.columns)}, got={val_arr.shape[1]}"
      ])

    row_count = int(ts_arr.shape[0])
    if row_count == 0:
      return InsertResult(rows=0, elapsed_s=0.0, rows_per_sec=0.0)

    t0 = time.perf_counter()

    if store_raw:
      for i in range(row_count):
        self._store.append(
            stream_name, int(ts_arr[i]), val_arr[i].tolist(),
        )

    self._ensure_session_deployed()
    self._feed_session_buffer(stream_name, ts_arr, val_arr)

    elapsed = time.perf_counter() - t0
    rps = row_count / elapsed if elapsed > 0 else float('inf')
    return InsertResult(rows=row_count, elapsed_s=elapsed, rows_per_sec=rps)

  def insert_dataframe(
      self,
      stream_name: str,
      dataframe: Any,
      column_map: Optional[Dict[str, str]] = None,
      *,
      store_raw: bool = True,
  ) -> InsertResult:
    schema = self._catalog.lookup_stream(stream_name)
    if schema is None:
      raise SqlError([f"Unknown stream: {stream_name}"])

    cmap = column_map or {}
    t0 = time.perf_counter()

    # --- Fast path: pandas DataFrame with numpy batch API ---
    if _HAS_NUMPY and hasattr(dataframe, "values") and hasattr(dataframe, "columns"):
      row_count = self._insert_dataframe_fast(
          stream_name, dataframe, schema, cmap, store_raw,
      )
    else:
      row_count = self._insert_dataframe_slow(
          stream_name, dataframe, schema, cmap, store_raw,
      )

    elapsed = time.perf_counter() - t0
    rps = row_count / elapsed if elapsed > 0 else float('inf')
    return InsertResult(rows=row_count, elapsed_s=elapsed, rows_per_sec=rps)

  def _insert_dataframe_fast(
      self,
      stream_name: str,
      dataframe: Any,
      schema: StreamSchema,
      cmap: Dict[str, str],
      store_raw: bool,
  ) -> int:
    """Optimized path for pandas DataFrames using numpy arrays + C++ batch feed."""
    # Resolve time column
    time_col = None
    for candidate in ("time", "timestamp", "ts"):
      if candidate in dataframe.columns:
        time_col = candidate
        break

    # Build column index mapping: schema column order -> DataFrame column index
    col_names = [cmap.get(col.name, col.name) for col in schema.columns]
    value_cols = dataframe[col_names].to_numpy(dtype=np.float64)

    if time_col is not None:
      timestamps = dataframe[time_col].to_numpy(dtype=np.int64)
    else:
      base = self._next_timestamp()
      timestamps = np.arange(base, base + len(dataframe), dtype=np.int64)
      self._last_timestamp = int(timestamps[-1]) if len(timestamps) > 0 else base

    row_count = len(timestamps)

    # Store raw stream data if requested (needed for backfill on view creation
    # and for multi-source interleaved replay).
    if store_raw:
      for i in range(row_count):
        self._store.append(stream_name, int(timestamps[i]), value_cols[i].tolist())

    self._ensure_session_deployed()
    self._feed_session_buffer(stream_name, timestamps, value_cols)

    return row_count

  # --- Consolidated-session helpers ---------------------------------

  def _ensure_session_deployed(self) -> None:
    """Lazily compile and deploy the consolidated session.

    Raises :class:`SqlError` if the catalog cannot be represented as a
    single Program (e.g. unknown source). Views referencing tables are
    supported — the table's name gets its own port on the session Input
    and INSERT INTO table routes via that port.
    """
    if self._session_pipeline is not None:
      return
    if not self._catalog.views():
      return  # no views → nothing to deploy
    snapshot = self._catalog.snapshot()
    result = native.compile_session(snapshot)
    errors = result.get("errors", [])
    if errors:
      raise SqlError([e.get("message", str(e)) for e in errors])

    view_terminals = dict(result.get("view_terminals", {}))
    self._session_op_to_view = {
        op_id: view_name for view_name, op_id in view_terminals.items()
    }
    self._session_stream_port = dict(result.get("base_stream_ports", {}))

    op_ids = list(view_terminals.values())
    self._session_pipeline = self._runner.deploy_session(
        result["program_json"], op_ids,
    )
    self._backfill_session_from_store()

  def _backfill_session_from_store(self) -> None:
    """Replay stored base-stream data through a freshly deployed
    session so newly registered views inherit history from their
    source streams (e.g. after DROP+recreate or when a view is
    created after inserts)."""
    events: List[Tuple[int, int, str, List[float]]] = []
    stream_idx = 0
    for stream, port in self._session_stream_port.items():
      for msg in self._store.read(stream):
        events.append((int(msg.timestamp), stream_idx, port,
                        [float(v) for v in msg.values]))
      stream_idx += 1
    if not events:
      return
    events.sort(key=lambda e: (e[0], e[1]))
    # Reverse-map port → stream so `_feed_session_row` can re-resolve
    # the port from the stream name. Avoids bypassing the helper for
    # this cold path.
    port_to_stream = {port: s for s, port in self._session_stream_port.items()}
    for ts, _idx, port, values in events:
      stream = port_to_stream.get(port, "")
      self._feed_session_row(stream, ts, values)

  def _invalidate_session(self) -> None:
    """Tear down the consolidated session so the next insert rebuilds
    it from the current catalog. Called after every DDL."""
    self._session_pipeline = None
    self._session_op_to_view = {}
    self._session_stream_port = {}

  def _feed_session_row(
      self,
      stream_name: str,
      timestamp: int,
      values: List[float],
  ) -> None:
    """Feed a single row into the session pipeline and dispatch any
    outputs to each view terminal's store/subscribers. No-op if the
    session hasn't been deployed yet."""
    if self._session_pipeline is None:
      return
    port = self._session_stream_port.get(stream_name, "i1")
    for out in self._session_pipeline.feed(timestamp, values, port):
      view_name = self._session_op_to_view.get(out.operator_id)
      if view_name is None:
        continue
      self._append_and_propagate(
          view_name, int(out.timestamp),
          [float(v) for v in out.values],
      )

  def _feed_session_buffer(
      self,
      stream_name: str,
      timestamps: Any,
      value_cols: Any,
  ) -> None:
    """Batched equivalent of :meth:`_feed_session_row`. Uses
    ``Program::receive_buffer`` via the pybind zero-copy path."""
    if self._session_pipeline is None:
      return
    port = self._session_stream_port.get(stream_name, "i1")
    outputs = self._runner.feed_session_buffer(
        self._session_pipeline, timestamps, value_cols, port=port,
    )
    for out in outputs:
      view_name = self._session_op_to_view.get(out.operator_id)
      if view_name is None:
        continue
      self._append_and_propagate(view_name, out.timestamp, out.values)

  def _insert_dataframe_slow(
      self,
      stream_name: str,
      dataframe: Any,
      schema: StreamSchema,
      cmap: Dict[str, str],
      store_raw: bool,
  ) -> int:
    """Row-by-row path for list-of-dicts and other iterables.

    Each row is stored (honouring ``store_raw``) and fed to the
    consolidated session Program; outputs are demuxed to each
    materialized view's store/subscribers.
    """
    self._ensure_session_deployed()
    time_col: Optional[str] = None
    row_count = 0

    for row in dataframe:
      if isinstance(row, dict):
        if time_col is None:
          for candidate in ("time", "timestamp", "ts"):
            if candidate in row:
              time_col = candidate
              break
        if time_col and time_col in row:
          timestamp = int(row[time_col])
        else:
          timestamp = self._next_timestamp()
        values = [float(row[cmap.get(col.name, col.name)]) for col in schema.columns]
      else:
        timestamp = self._next_timestamp()
        values = [float(v) for v in row]

      if store_raw:
        self._append_and_propagate(stream_name, timestamp, values)
      self._feed_session_row(stream_name, timestamp, values)
      row_count += 1

    return row_count

  def get_catalog(self) -> InMemoryCatalog:
    return self._catalog

  def get_store(self) -> InMemoryStreamStore:
    return self._store

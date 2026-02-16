"""User-facing Python runtime API for rtbot-sql."""

from __future__ import annotations

import json
import re
import time
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .catalog import InMemoryCatalog, StreamSchema, ViewMeta
from .compiler import compile_select_to_program, compile_sql, native
from .formatter import format_rows
from .jupyter import show_graph as display_graph
from .pipeline_runner import LocalPipelineRunner
from .stream_store import InMemoryStreamStore


class SqlError(RuntimeError):
  def __init__(self, errors: Sequence[Any]):
    self.errors = list(errors)
    messages = [getattr(err, "message", str(err)) for err in self.errors]
    super().__init__("; ".join(messages) if messages else "SQL error")


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

    self._view_pipelines: Dict[str, str] = {}
    self._dependencies: Dict[str, set] = defaultdict(set)

    self._last_timestamp = int(time.time() * 1000)

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
    normalized = self._normalize_sql(sql)

    result = compile_sql(normalized, self._catalog.snapshot())
    if result.has_errors():
      raise SqlError(result.errors)

    statement = result.statement_type
    if statement == native.StatementType.CREATE_STREAM:
      self._handle_create_stream(result)
      return None

    if statement == native.StatementType.INSERT:
      self._handle_insert(result)
      return None

    if statement in (
        native.StatementType.CREATE_VIEW,
        native.StatementType.CREATE_MATERIALIZED_VIEW,
    ):
      self._handle_create_view(result, statement)
      return None

    if statement == native.StatementType.DROP:
      self._handle_drop(result)
      return None

    if statement == native.StatementType.SELECT:
      return self._handle_select(normalized, result)

    raise SqlError([f"Unsupported statement type: {statement}"])

  def _handle_create_stream(self, result: native.CompilationResult) -> None:
    self._catalog.register_stream(result.entity_name, result.stream_schema)

  def _handle_insert(self, result: native.CompilationResult) -> None:
    stream_name = result.entity_name
    schema = self._catalog.lookup_stream(stream_name)
    if schema is None:
      raise SqlError([f"Unknown stream: {stream_name}"])

    payload = [float(v) for v in result.insert_payload]
    if schema.columns and len(payload) != len(schema.columns):
      raise SqlError([
          f"INSERT payload length mismatch for {stream_name}: expected {len(schema.columns)}, got {len(payload)}"
      ])

    self._append_and_propagate(stream_name, self._next_timestamp(), payload)

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
    for source in view_meta.source_streams:
      self._dependencies[source].add(name)

    pipeline_id = self._runner.deploy(result.program_json, view_meta.source_streams, {"output_stream": name})
    self._view_pipelines[name] = pipeline_id

    # Backfill the new view from already-known source data.
    for source in view_meta.source_streams:
      for msg in self._store.read(source):
        outputs = self._runner.feed(pipeline_id, msg.timestamp, msg.values)
        for out in outputs:
          self._append_and_propagate(name, out.timestamp, out.values)

  def _handle_drop(self, result: native.CompilationResult) -> None:
    name = result.drop_entity_name

    if name in self._view_pipelines:
      self._runner.destroy(self._view_pipelines[name])
      self._view_pipelines.pop(name, None)

    self._catalog.drop(name)
    self._store.clear(name)

    for dependents in self._dependencies.values():
      dependents.discard(name)

    self._dependencies.pop(name, None)

  def _append_and_propagate(self, stream_name: str, timestamp: int, values: List[float]) -> None:
    self._store.append(stream_name, timestamp, values)

    view = self._catalog.lookup_view(stream_name)
    if (
        view is not None
        and view.view_type == native.ViewType.KEYED
        and 0 <= view.key_index < len(values)
    ):
      self._catalog.add_key(stream_name, float(values[view.key_index]))

    for dependent in list(self._dependencies.get(stream_name, set())):
      pipeline_id = self._view_pipelines.get(dependent)
      if pipeline_id is None:
        continue
      outputs = self._runner.feed(pipeline_id, timestamp, values)
      for out in outputs:
        self._append_and_propagate(dependent, out.timestamp, out.values)

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

  def _execute_tier1(self, sql: str, result: native.CompilationResult):
    source = result.source_streams[0] if result.source_streams else ""
    if not source:
      return format_rows([], {})

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
    return format_rows(rows, effective_field_map)

  def _execute_with_pipeline(self, sql: str, result: native.CompilationResult):
    runtime_result = result
    if not runtime_result.program_json:
      runtime_result = compile_select_to_program(sql, self._catalog.snapshot())
      if runtime_result.has_errors():
        raise SqlError(runtime_result.errors)

    source = runtime_result.source_streams[0] if runtime_result.source_streams else ""
    if not source:
      return format_rows([], dict(runtime_result.field_map))

    outputs = self._runner.run_once(runtime_result.program_json, self._store.read(source))
    rows = [list(out.values) for out in outputs]

    limit = _extract_limit(sql)
    if limit is not None:
      rows = rows[:limit]

    return format_rows(rows, dict(runtime_result.field_map))

  def _handle_select(self, sql: str, result: native.CompilationResult):
    if result.select_tier == native.SelectTier.TIER1_READ:
      return self._execute_tier1(sql, result)
    return self._execute_with_pipeline(sql, result)

  def explain(self, sql: str) -> Dict[str, Any]:
    normalized = self._normalize_sql(sql)
    result = compile_sql(normalized, self._catalog.snapshot())

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

  def insert_dataframe(self, stream_name: str, dataframe: Any) -> None:
    schema = self._catalog.lookup_stream(stream_name)
    if schema is None:
      raise SqlError([f"Unknown stream: {stream_name}"])

    if hasattr(dataframe, "iterrows"):
      for _, row in dataframe.iterrows():
        timestamp = int(row["time"]) if "time" in row else self._next_timestamp()
        values = [float(row[col.name]) for col in schema.columns]
        self._append_and_propagate(stream_name, timestamp, values)
      return

    for row in dataframe:
      if isinstance(row, dict):
        timestamp = int(row.get("time", self._next_timestamp()))
        values = [float(row[col.name]) for col in schema.columns]
      else:
        timestamp = self._next_timestamp()
        values = [float(v) for v in row]
      self._append_and_propagate(stream_name, timestamp, values)

  def get_catalog(self) -> InMemoryCatalog:
    return self._catalog

  def get_store(self) -> InMemoryStreamStore:
    return self._store

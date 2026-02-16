"""In-memory catalog for the Python runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from .compiler import native


@dataclass
class ColumnDef:
  name: str
  index: int


@dataclass
class StreamSchema:
  name: str
  columns: List[ColumnDef] = field(default_factory=list)


@dataclass
class ViewMeta:
  name: str
  entity_type: Any
  view_type: Any
  field_map: Dict[str, int] = field(default_factory=dict)
  source_streams: List[str] = field(default_factory=list)
  program_json: str = ""
  output_stream: str = ""
  per_key_prefix: str = ""
  known_keys: List[float] = field(default_factory=list)
  key_index: int = -1


@dataclass
class TableSchema:
  name: str
  columns: List[ColumnDef] = field(default_factory=list)
  changelog_stream: str = ""


def _to_column_def(obj: Any) -> ColumnDef:
  if isinstance(obj, ColumnDef):
    return ColumnDef(obj.name, obj.index)
  if isinstance(obj, native.ColumnDef):
    return ColumnDef(obj.name, int(obj.index))
  if isinstance(obj, dict):
    return ColumnDef(str(obj["name"]), int(obj["index"]))
  raise TypeError(f"Unsupported column definition: {type(obj)!r}")


def _to_stream_schema(name: str, obj: Any) -> StreamSchema:
  if isinstance(obj, StreamSchema):
    return StreamSchema(name=name or obj.name, columns=[_to_column_def(c) for c in obj.columns])
  if isinstance(obj, native.StreamSchema):
    return StreamSchema(name=name or obj.name, columns=[_to_column_def(c) for c in obj.columns])
  if isinstance(obj, dict):
    cols = [_to_column_def(c) for c in obj.get("columns", [])]
    return StreamSchema(name=name or str(obj.get("name", name)), columns=cols)
  raise TypeError(f"Unsupported stream schema: {type(obj)!r}")


def _to_view_meta(name: str, obj: Any) -> ViewMeta:
  if isinstance(obj, ViewMeta):
    return ViewMeta(
        name=name or obj.name,
        entity_type=obj.entity_type,
        view_type=obj.view_type,
        field_map=dict(obj.field_map),
        source_streams=list(obj.source_streams),
        program_json=obj.program_json,
        output_stream=obj.output_stream,
        per_key_prefix=obj.per_key_prefix,
        known_keys=list(obj.known_keys),
        key_index=int(obj.key_index),
    )
  if isinstance(obj, native.ViewMeta):
    return ViewMeta(
        name=name or obj.name,
        entity_type=obj.entity_type,
        view_type=obj.view_type,
        field_map=dict(obj.field_map),
        source_streams=list(obj.source_streams),
        program_json=obj.program_json,
        output_stream=obj.output_stream,
        per_key_prefix=obj.per_key_prefix,
        known_keys=list(obj.known_keys),
        key_index=int(obj.key_index),
    )
  if isinstance(obj, dict):
    return ViewMeta(
        name=name or str(obj.get("name", name)),
        entity_type=obj.get("entity_type", native.EntityType.VIEW),
        view_type=obj.get("view_type", native.ViewType.SCALAR),
        field_map=dict(obj.get("field_map", {})),
        source_streams=list(obj.get("source_streams", [])),
        program_json=str(obj.get("program_json", "")),
        output_stream=str(obj.get("output_stream", "")),
        per_key_prefix=str(obj.get("per_key_prefix", "")),
        known_keys=[float(v) for v in obj.get("known_keys", [])],
        key_index=int(obj.get("key_index", -1)),
    )
  raise TypeError(f"Unsupported view metadata: {type(obj)!r}")


def _to_table_schema(name: str, obj: Any) -> TableSchema:
  if isinstance(obj, TableSchema):
    return TableSchema(
        name=name or obj.name,
        columns=[_to_column_def(c) for c in obj.columns],
        changelog_stream=obj.changelog_stream,
    )
  if isinstance(obj, native.TableSchema):
    return TableSchema(
        name=name or obj.name,
        columns=[_to_column_def(c) for c in obj.columns],
        changelog_stream=obj.changelog_stream,
    )
  if isinstance(obj, dict):
    return TableSchema(
        name=name or str(obj.get("name", name)),
        columns=[_to_column_def(c) for c in obj.get("columns", [])],
        changelog_stream=str(obj.get("changelog_stream", "")),
    )
  raise TypeError(f"Unsupported table schema: {type(obj)!r}")


def _column_to_native(col: ColumnDef) -> native.ColumnDef:
  return native.ColumnDef(col.name, int(col.index))


def _stream_to_native(schema: StreamSchema) -> native.StreamSchema:
  out = native.StreamSchema()
  out.name = schema.name
  out.columns = [_column_to_native(c) for c in schema.columns]
  return out


def _view_to_native(meta: ViewMeta) -> native.ViewMeta:
  out = native.ViewMeta()
  out.name = meta.name
  out.entity_type = meta.entity_type
  out.view_type = meta.view_type
  out.field_map = dict(meta.field_map)
  out.source_streams = list(meta.source_streams)
  out.program_json = meta.program_json
  out.output_stream = meta.output_stream
  out.per_key_prefix = meta.per_key_prefix
  out.known_keys = [float(v) for v in meta.known_keys]
  out.key_index = int(meta.key_index)
  return out


def _table_to_native(schema: TableSchema) -> native.TableSchema:
  out = native.TableSchema()
  out.name = schema.name
  out.columns = [_column_to_native(c) for c in schema.columns]
  out.changelog_stream = schema.changelog_stream
  return out


class InMemoryCatalog:
  def __init__(self) -> None:
    self._streams: Dict[str, StreamSchema] = {}
    self._views: Dict[str, ViewMeta] = {}
    self._tables: Dict[str, TableSchema] = {}

  def register_stream(self, name: str, schema: Any) -> None:
    self._streams[name] = _to_stream_schema(name, schema)

  def register_view(self, name: str, meta: Any) -> None:
    self._views[name] = _to_view_meta(name, meta)

  def register_table(self, name: str, schema: Any) -> None:
    self._tables[name] = _to_table_schema(name, schema)

  def lookup_stream(self, name: str) -> Optional[StreamSchema]:
    return self._streams.get(name)

  def lookup_view(self, name: str) -> Optional[ViewMeta]:
    return self._views.get(name)

  def lookup_table(self, name: str) -> Optional[TableSchema]:
    return self._tables.get(name)

  def resolve_entity(self, name: str) -> Optional[Any]:
    if name in self._streams:
      return native.EntityType.STREAM
    if name in self._views:
      return self._views[name].entity_type
    if name in self._tables:
      return native.EntityType.TABLE
    return None

  def snapshot(self) -> native.CatalogSnapshot:
    snap = native.CatalogSnapshot()
    snap.streams = {name: _stream_to_native(schema) for name, schema in self._streams.items()}
    snap.views = {name: _view_to_native(meta) for name, meta in self._views.items()}
    snap.tables = {name: _table_to_native(schema) for name, schema in self._tables.items()}
    return snap

  def add_key(self, view_name: str, key: float) -> None:
    view = self._views.get(view_name)
    if view is None:
      return
    key_value = float(key)
    if key_value not in view.known_keys:
      view.known_keys.append(key_value)

  def get_known_keys(self, view_name: str) -> set:
    view = self._views.get(view_name)
    if view is None:
      return set()
    return set(view.known_keys)

  def drop_stream(self, name: str) -> None:
    self._streams.pop(name, None)

  def drop_view(self, name: str) -> None:
    self._views.pop(name, None)

  def drop_table(self, name: str) -> None:
    self._tables.pop(name, None)

  def drop(self, name: str) -> bool:
    removed = False
    if name in self._streams:
      self._streams.pop(name, None)
      removed = True
    if name in self._views:
      self._views.pop(name, None)
      removed = True
    if name in self._tables:
      self._tables.pop(name, None)
      removed = True
    return removed

  def streams(self) -> Dict[str, StreamSchema]:
    return dict(self._streams)

  def views(self) -> Dict[str, ViewMeta]:
    return dict(self._views)

  def tables(self) -> Dict[str, TableSchema]:
    return dict(self._tables)

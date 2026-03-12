"""Thin wrappers over the native rtbot-sql extension."""

from __future__ import annotations

import importlib.util
import sys
from importlib.machinery import ExtensionFileLoader, ModuleSpec
from pathlib import Path
from typing import Any

try:
  from . import _rtbot_sql_native as native  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - fallback for Bazel runfiles layout
  try:
    import _rtbot_sql_native as native  # type: ignore
  except ImportError:
    if not sys.platform.startswith("win"):
      raise
    ext_path = Path(__file__).with_name("_rtbot_sql_native.so")
    if not ext_path.exists():
      raise
    loader = ExtensionFileLoader("rtbot_sql._rtbot_sql_native", str(ext_path))
    spec = ModuleSpec("rtbot_sql._rtbot_sql_native", loader, origin=str(ext_path))
    native = importlib.util.module_from_spec(spec)
    loader.exec_module(native)


def compile_sql(sql: str, catalog_snapshot: Any) -> Any:
  return native.compile_sql(sql, catalog_snapshot)


def validate_sql(sql: str) -> dict:
  return native.validate_sql(sql)


def compile_select_to_program(sql: str, catalog_snapshot: Any) -> Any:
  """Compile a SELECT by wrapping it in CREATE MATERIALIZED VIEW.

  compile_sql() only emits program_json for tier-3 SELECT queries. For tier-2,
  this wrapper forces graph compilation and lets the Python runtime execute the
  query locally through the native pipeline runner.
  """
  select_sql = sql.strip().rstrip(";")
  wrapped_sql = f"CREATE MATERIALIZED VIEW __rtbot_sql_tmp AS {select_sql}"
  return native.compile_sql(wrapped_sql, catalog_snapshot)

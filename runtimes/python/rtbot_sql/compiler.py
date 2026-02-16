"""Thin wrappers over the native rtbot-sql extension."""

from __future__ import annotations

try:
  from . import _rtbot_sql_native as native
except ImportError:  # pragma: no cover - fallback for Bazel runfiles layout
  import _rtbot_sql_native as native  # type: ignore


def compile_sql(sql: str, catalog_snapshot: native.CatalogSnapshot) -> native.CompilationResult:
  return native.compile_sql(sql, catalog_snapshot)


def validate_sql(sql: str) -> dict:
  return native.validate_sql(sql)


def compile_select_to_program(sql: str, catalog_snapshot: native.CatalogSnapshot) -> native.CompilationResult:
  """Compile a SELECT by wrapping it in CREATE MATERIALIZED VIEW.

  compile_sql() only emits program_json for tier-3 SELECT queries. For tier-2,
  this wrapper forces graph compilation and lets the Python runtime execute the
  query locally through the native pipeline runner.
  """
  select_sql = sql.strip().rstrip(";")
  wrapped_sql = f"CREATE MATERIALIZED VIEW __rtbot_sql_tmp AS {select_sql}"
  return native.compile_sql(wrapped_sql, catalog_snapshot)

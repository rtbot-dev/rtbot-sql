"""Cross-runtime tests: Python runtime runner.

Loads queries.json and executes each test case through the Python RtBotSql
runtime, comparing results against expected values.
"""

import json
import os
import unittest

import rtbot_sql
from rtbot_sql import compiler

TOLERANCE = 1e-9
QUERIES_PATH = os.path.join(os.path.dirname(__file__), "queries.json")


def _load_queries():
  with open(QUERIES_PATH) as f:
    return json.load(f)


def _columns_and_rows(result):
  if isinstance(result, dict):
    return list(result["columns"]), [list(row) for row in result["rows"]]
  if hasattr(result, "columns") and hasattr(result, "values"):
    return list(result.columns), result.values.tolist()
  raise TypeError(f"Unsupported result type: {type(result)!r}")


def _approx_equal(a, b, tol=TOLERANCE):
  if isinstance(a, float) and isinstance(b, float):
    return abs(a - b) < tol
  return a == b


def _rows_match(actual, expected, tol=TOLERANCE):
  if len(actual) != len(expected):
    return False
  for a_row, e_row in zip(actual, expected):
    if len(a_row) != len(e_row):
      return False
    for a_val, e_val in zip(a_row, e_row):
      if not _approx_equal(a_val, e_val, tol):
        return False
  return True


def _rows_match_unordered(actual, expected, key_index=0, tol=TOLERANCE):
  if len(actual) != len(expected):
    return False
  actual_sorted = sorted(actual, key=lambda r: r[key_index])
  expected_sorted = sorted(expected, key=lambda r: r[key_index])
  return _rows_match(actual_sorted, expected_sorted, tol)


def _field_map_matches(actual_columns, expected_field_map):
  expected_columns = [
      name
      for name, _ in sorted(expected_field_map.items(), key=lambda kv: kv[1])
  ]
  return actual_columns == expected_columns


class PythonCrossRuntimeTest(unittest.TestCase):
  """Runs cross-runtime queries.json test cases via the Python RtBotSql runtime."""

  @classmethod
  def setUpClass(cls):
    cls.queries = _load_queries()

  def _make_runtime(self):
    return rtbot_sql.RtBotSql()

  def _run_setup(self, rt, case):
    for stmt in case.get("setup", []):
      rt.execute(stmt)
    if "materialized_view" in case:
      rt.execute(case["materialized_view"])


class TestExecutionCases(PythonCrossRuntimeTest):

  def _run_case(self, case):
    rt = self._make_runtime()
    self._run_setup(rt, case)
    result = rt.execute(case["query"])
    columns, rows = _columns_and_rows(result)
    return columns, rows

  def test_simple_select_limit(self):
    case = next(c for c in self.queries if c["name"] == "simple_select_limit")
    columns, rows = self._run_case(case)
    self.assertTrue(
        _field_map_matches(columns, case["expected"]["field_map"]),
        f"Column mismatch: {columns}",
    )
    self.assertTrue(
        _rows_match(rows, case["expected"]["rows"]),
        f"Row mismatch:\n  actual:   {rows}\n  expected: {case['expected']['rows']}",
    )

  def test_select_limit_truncates(self):
    case = next(c for c in self.queries if c["name"] == "select_limit_truncates")
    columns, rows = self._run_case(case)
    self.assertTrue(_field_map_matches(columns, case["expected"]["field_map"]))
    self.assertTrue(
        _rows_match(rows, case["expected"]["rows"]),
        f"Row mismatch:\n  actual:   {rows}\n  expected: {case['expected']['rows']}",
    )

  def test_where_filter(self):
    case = next(c for c in self.queries if c["name"] == "where_filter")
    columns, rows = self._run_case(case)
    self.assertTrue(_field_map_matches(columns, case["expected"]["field_map"]))
    self.assertTrue(
        _rows_match(rows, case["expected"]["rows"]),
        f"Row mismatch:\n  actual:   {rows}\n  expected: {case['expected']['rows']}",
    )

  def test_where_filter_and(self):
    case = next(c for c in self.queries if c["name"] == "where_filter_and")
    columns, rows = self._run_case(case)
    self.assertTrue(_field_map_matches(columns, case["expected"]["field_map"]))
    self.assertTrue(
        _rows_match(rows, case["expected"]["rows"]),
        f"Row mismatch:\n  actual:   {rows}\n  expected: {case['expected']['rows']}",
    )

  def test_expression_in_select(self):
    case = next(c for c in self.queries if c["name"] == "expression_in_select")
    columns, rows = self._run_case(case)
    self.assertTrue(_field_map_matches(columns, case["expected"]["field_map"]))
    self.assertTrue(
        _rows_match(rows, case["expected"]["rows"]),
        f"Row mismatch:\n  actual:   {rows}\n  expected: {case['expected']['rows']}",
    )

  def test_group_by_sum_count(self):
    case = next(c for c in self.queries if c["name"] == "group_by_sum_count")
    columns, rows = self._run_case(case)
    expected = case["expected"]
    self.assertTrue(_field_map_matches(columns, expected["field_map"]))
    self.assertTrue(
        _rows_match_unordered(rows, expected["rows_unordered"]),
        f"Row mismatch (unordered):\n  actual:   {rows}\n  expected: {expected['rows_unordered']}",
    )

  def test_group_by_avg(self):
    case = next(c for c in self.queries if c["name"] == "group_by_avg")
    columns, rows = self._run_case(case)
    expected = case["expected"]
    self.assertTrue(_field_map_matches(columns, expected["field_map"]))
    self.assertTrue(
        _rows_match_unordered(rows, expected["rows_unordered"]),
        f"Row mismatch (unordered):\n  actual:   {rows}\n  expected: {expected['rows_unordered']}",
    )

  def test_insert_and_read_back(self):
    case = next(c for c in self.queries if c["name"] == "insert_and_read_back")
    columns, rows = self._run_case(case)
    self.assertTrue(_field_map_matches(columns, case["expected"]["field_map"]))
    self.assertTrue(
        _rows_match(rows, case["expected"]["rows"]),
        f"Row mismatch:\n  actual:   {rows}\n  expected: {case['expected']['rows']}",
    )


class TestErrorCases(PythonCrossRuntimeTest):

  def test_error_unknown_column(self):
    case = next(c for c in self.queries if c["name"] == "error_unknown_column")
    rt = self._make_runtime()
    self._run_setup(rt, case)
    with self.assertRaises(rtbot_sql.runtime.SqlError):
      rt.execute(case["query"])

  def test_error_unknown_table(self):
    case = next(c for c in self.queries if c["name"] == "error_unknown_table")
    rt = self._make_runtime()
    with self.assertRaises(rtbot_sql.runtime.SqlError):
      rt.execute(case["query"])

  def test_error_syntax(self):
    case = next(c for c in self.queries if c["name"] == "error_syntax")
    rt = self._make_runtime()
    with self.assertRaises(rtbot_sql.runtime.SqlError):
      rt.execute(case["query"])


class TestDropStream(PythonCrossRuntimeTest):

  def test_drop_removes_entity(self):
    case = next(c for c in self.queries if c["name"] == "drop_stream")
    rt = self._make_runtime()
    self._run_setup(rt, case)

    # Stream should exist before drop
    self.assertIsNotNone(rt.get_catalog().lookup_stream(case["verify_dropped"]))

    rt.execute(case["drop"])

    # Stream should be gone after drop
    self.assertIsNone(rt.get_catalog().lookup_stream(case["verify_dropped"]))


class TestCompilationDeterminism(PythonCrossRuntimeTest):

  def test_deterministic_compilation(self):
    case = next(c for c in self.queries if c["name"] == "compilation_determinism")
    rt = self._make_runtime()
    self._run_setup(rt, case)
    snapshot = rt.get_catalog().snapshot()

    for query in case["queries"]:
      # Compile twice with same inputs
      r1 = compiler.compile_sql(query, snapshot)
      r2 = compiler.compile_sql(query, snapshot)

      self.assertFalse(r1.has_errors(), f"Compilation failed for: {query}")
      self.assertFalse(r2.has_errors(), f"Compilation failed for: {query}")

      # program_json must be byte-identical
      self.assertEqual(
          r1.program_json,
          r2.program_json,
          f"Non-deterministic program_json for: {query}",
      )

      # field_map must be identical
      self.assertEqual(
          dict(r1.field_map),
          dict(r2.field_map),
          f"Non-deterministic field_map for: {query}",
      )

      # select_tier must be identical
      self.assertEqual(
          r1.select_tier,
          r2.select_tier,
          f"Non-deterministic select_tier for: {query}",
      )


if __name__ == "__main__":
  unittest.main()

"""rtbot-sql Python runtime."""

from .catalog import ColumnDef, InMemoryCatalog, StreamSchema, TableSchema, ViewMeta
from .compiler import compile_sql, native, validate_sql
from .formatter import normalize_time_column
from .pipeline_runner import InputMessage, LocalPipelineRunner, PipelineOutput
from .runtime import BacktestResult, InsertResult, RtBotSql, SqlError, bind_sql_params
from .stream_store import InMemoryStreamStore, Message


_default = RtBotSql()


def execute(sql: str, params=None):
  return _default.execute(sql, params=params)


def insert_dataframe(stream_name: str, dataframe):
  return _default.insert_dataframe(stream_name, dataframe)


def backtest(stream_name: str, timestamps, value_columns, *, views=None):
  return _default.backtest(stream_name, timestamps, value_columns, views=views)


def sweep(stream_name: str, timestamps, value_columns, query: str, param_grid):
  return _default.sweep(stream_name, timestamps, value_columns, query, param_grid)


def show_graph(view_name: str):
  return _default.show_graph(view_name)


def explain(sql: str):
  return _default.explain(sql)


__all__ = [
    "ColumnDef",
    "StreamSchema",
    "ViewMeta",
    "TableSchema",
    "InMemoryCatalog",
    "InMemoryStreamStore",
    "Message",
    "InputMessage",
    "PipelineOutput",
    "LocalPipelineRunner",
    "InsertResult",
    "BacktestResult",
    "RtBotSql",
    "SqlError",
    "bind_sql_params",
    "normalize_time_column",
    "compile_sql",
    "validate_sql",
    "native",
    "execute",
    "insert_dataframe",
    "backtest",
    "sweep",
    "show_graph",
    "explain",
]

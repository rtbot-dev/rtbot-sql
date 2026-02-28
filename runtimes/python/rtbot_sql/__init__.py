"""rtbot-sql Python runtime."""

from .catalog import ColumnDef, InMemoryCatalog, StreamSchema, TableSchema, ViewMeta
from .compiler import compile_sql, native, validate_sql
from .formatter import normalize_time_column
from .pipeline_runner import InputMessage, LocalPipelineRunner, PipelineOutput
from .runtime import InsertResult, RtBotSql, SqlError
from .stream_store import InMemoryStreamStore, Message


_default = RtBotSql()


def execute(sql: str):
  return _default.execute(sql)


def insert_dataframe(stream_name: str, dataframe):
  return _default.insert_dataframe(stream_name, dataframe)


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
    "RtBotSql",
    "SqlError",
    "normalize_time_column",
    "compile_sql",
    "validate_sql",
    "native",
    "execute",
    "insert_dataframe",
    "show_graph",
    "explain",
]

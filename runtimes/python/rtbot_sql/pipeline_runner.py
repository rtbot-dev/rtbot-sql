"""Local pipeline runner backed by native rtbot::Program execution."""

from __future__ import annotations

import array
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Tuple

from .compiler import native
from .stream_store import Message

try:
  import numpy as np
  _HAS_NUMPY = True
except ImportError:
  _HAS_NUMPY = False


def coerce_input_buffers(timestamps: Any, values_2d: Any) -> Tuple[Any, Any, int, int]:
  """Coerce timestamps (1D int64) and values_2d (2D float64) into contiguous
  C-buffer objects (numpy arrays when available, stdlib array.array otherwise)."""
  if _HAS_NUMPY:
    ts_arr = np.ascontiguousarray(timestamps, dtype=np.int64)
    val_arr = np.ascontiguousarray(values_2d, dtype=np.float64)
    if ts_arr.ndim != 1:
      raise ValueError("timestamps must be 1D")
    if val_arr.ndim != 2:
      raise ValueError("value_columns must be 2D")
    if ts_arr.shape[0] != val_arr.shape[0]:
      raise ValueError(
          f"row count mismatch between timestamps ({ts_arr.shape[0]}) "
          f"and value_columns ({val_arr.shape[0]})"
      )
    return ts_arr, val_arr, int(ts_arr.shape[0]), int(val_arr.shape[1])

  if (
      isinstance(timestamps, array.array)
      and timestamps.typecode == "q"
      and isinstance(values_2d, array.array)
      and values_2d.typecode == "d"
  ):
    nrows = len(timestamps)
    if nrows > 0 and len(values_2d) % nrows != 0:
      raise ValueError(
          f"row count mismatch between timestamps ({nrows}) "
          f"and flat value buffer ({len(values_2d)})"
      )
    ncols = (len(values_2d) // nrows) if nrows > 0 else 0
    return timestamps, values_2d, nrows, ncols

  ts_list = [int(t) for t in timestamps]
  if any(not isinstance(r, (list, tuple)) for r in values_2d):
    raise ValueError("value_columns must be 2D")
  rows_list = [[float(x) for x in row] for row in values_2d]
  nrows = len(ts_list)
  if nrows != len(rows_list):
    raise ValueError(
        f"row count mismatch between timestamps ({nrows}) "
        f"and value_columns ({len(rows_list)})"
    )
  ncols = len(rows_list[0]) if nrows > 0 else 0
  for r in rows_list:
    if len(r) != ncols:
      raise ValueError("all rows in value_columns must have the same length")
  ts_buf = array.array("q", ts_list)
  flat_vals = array.array("d", (x for r in rows_list for x in r))
  return ts_buf, flat_vals, nrows, ncols


@dataclass
class InputMessage:
  timestamp: int
  values: List[float]
  port: str = "i1"


@dataclass
class PipelineOutput:
  timestamp: int
  values: List[float]
  operator_id: str = ""
  port: str = ""


def _normalize_input(item: Any) -> InputMessage:
  if isinstance(item, InputMessage):
    return InputMessage(int(item.timestamp), [float(v) for v in item.values], item.port)
  if isinstance(item, Message):
    return InputMessage(int(item.timestamp), [float(v) for v in item.values], "i1")
  if isinstance(item, dict):
    return InputMessage(
        int(item["timestamp"]),
        [float(v) for v in item["values"]],
        str(item.get("port", "i1")),
    )
  if isinstance(item, (tuple, list)) and len(item) >= 2:
    port = "i1" if len(item) < 3 else str(item[2])
    return InputMessage(int(item[0]), [float(v) for v in item[1]], port)
  raise TypeError(f"Unsupported input message: {type(item)!r}")


class LocalPipelineRunner:
  """Thin wrapper over the native consolidated-session pipeline and the
  throw-away :meth:`run_once` program used for Tier-3 ephemeral SELECTs.

  The per-view pipeline registry was removed along with the per-view
  cascade — all view execution now runs through the single session
  program owned by :class:`rtbot_sql.RtBotSql`.
  """

  def run_once(
      self,
      program_json: str,
      input_messages: Iterable[Any],
  ) -> List[PipelineOutput]:
    """Build a one-shot rtbot Program, run ``input_messages`` through
    it, and return all emitted outputs. Used for Tier-3 ephemeral
    SELECTs; the Program is discarded when this function returns.
    """
    pipeline = native.NativePipeline(program_json)
    outputs: List[PipelineOutput] = []
    for raw in input_messages:
      msg = _normalize_input(raw)
      native_outputs = pipeline.feed(msg.timestamp, msg.values, msg.port)
      outputs.extend(
          PipelineOutput(
              timestamp=int(out.timestamp),
              values=[float(v) for v in out.values],
              operator_id=str(out.operator_id),
              port=str(out.port),
          )
          for out in native_outputs
      )
    return outputs

  # -- Consolidated-session pipeline ----------------------------------

  def deploy_session(
      self, program_json: str, op_ids: List[str],
  ) -> "native.NativeSessionPipeline":
    """Instantiate a :class:`NativeSessionPipeline` from the
    consolidated-session program JSON and register its output operator
    ids. The caller (typically :class:`rtbot_sql.RtBotSql`) owns the
    returned handle's lifetime.
    """
    session = native.NativeSessionPipeline(program_json)
    if op_ids:
      session.register_outputs(list(op_ids))
    return session

  def feed_session_buffer(
      self,
      session: "native.NativeSessionPipeline",
      timestamps: Any,
      values_2d: Any,
      port: str = "i1",
  ) -> List[PipelineOutput]:
    """Feed one batch through the consolidated session pipeline."""
    ts_buf, val_buf, _, _ = coerce_input_buffers(timestamps, values_2d)
    native_outputs = session.feed_buffer(ts_buf, val_buf, str(port))
    return [
        PipelineOutput(
            timestamp=int(out.timestamp),
            values=[float(v) for v in out.values],
            operator_id=str(out.operator_id),
            port=str(out.port),
        )
        for out in native_outputs
    ]

  def feed_session_buffer_columnar(
      self,
      session: "native.NativeSessionPipeline",
      timestamps: Any,
      values_2d: Any,
      port: str = "i1",
  ) -> Dict[str, Tuple[Any, Any]]:
    """Feed one batch through the consolidated session pipeline and return
    columnar buffers per output operator_id without per-row Python objects."""
    ts_buf, val_buf, _, _ = coerce_input_buffers(timestamps, values_2d)
    raw = session.feed_buffer_columnar(ts_buf, val_buf, str(port))
    out: Dict[str, Tuple[Any, Any]] = {}
    for op_id, (out_ts, out_vals) in raw.items():
      if _HAS_NUMPY:
        out[str(op_id)] = (np.asarray(out_ts), np.asarray(out_vals))
      else:
        out[str(op_id)] = (out_ts.tolist(), out_vals.tolist())
    return out

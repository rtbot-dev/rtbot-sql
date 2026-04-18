"""Local pipeline runner backed by native rtbot::Program execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, List

from .compiler import native
from .stream_store import Message

try:
  import numpy as np
  _HAS_NUMPY = True
except ImportError:
  _HAS_NUMPY = False


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
    if not _HAS_NUMPY:
      raise ImportError("feed_session_buffer requires numpy")
    native_outputs = session.feed_buffer(
        np.ascontiguousarray(timestamps, dtype=np.int64),
        np.ascontiguousarray(values_2d, dtype=np.float64),
        str(port),
    )
    return [
        PipelineOutput(
            timestamp=int(out.timestamp),
            values=[float(v) for v in out.values],
            operator_id=str(out.operator_id),
            port=str(out.port),
        )
        for out in native_outputs
    ]

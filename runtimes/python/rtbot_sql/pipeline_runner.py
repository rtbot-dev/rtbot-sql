"""Local pipeline runner backed by native rtbot::Program execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

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
  def __init__(self) -> None:
    self._pipelines: Dict[str, native.NativePipeline] = {}
    self._counter = 0

  def _next_id(self) -> str:
    self._counter += 1
    return f"pipeline_{self._counter}"

  def deploy(self, program_json: str, _input_streams: Any = None, _output_config: Any = None) -> str:
    pipeline_id = self._next_id()
    self._pipelines[pipeline_id] = native.NativePipeline(program_json)
    return pipeline_id

  def run_once(self, program_json: str, input_messages: Iterable[Any]) -> List[PipelineOutput]:
    pipeline = native.NativePipeline(program_json)
    return self._run_pipeline(pipeline, input_messages)

  def feed(
      self,
      pipeline_id: str,
      timestamp: int,
      values: List[float],
      port: str = "i1",
  ) -> List[PipelineOutput]:
    pipeline = self._pipelines[pipeline_id]
    native_outputs = pipeline.feed(int(timestamp), [float(v) for v in values], str(port))
    return [
        PipelineOutput(
            timestamp=int(out.timestamp),
            values=[float(v) for v in out.values],
            operator_id=str(out.operator_id),
            port=str(out.port),
        )
        for out in native_outputs
    ]

  def feed_batch(
      self,
      pipeline_id: str,
      timestamps: Any,
      values_2d: Any,
      port: str = "i1",
  ) -> List[PipelineOutput]:
    """Feed a batch of rows via numpy arrays (one C++ call for all rows).

    Requires numpy. Falls back to row-by-row ``feed`` when numpy is unavailable.
    """
    if not _HAS_NUMPY:
      raise ImportError("feed_batch requires numpy")
    pipeline = self._pipelines[pipeline_id]
    native_outputs = pipeline.feed_batch(
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

  def feed_buffer(
      self,
      pipeline_id: str,
      timestamps: Any,
      values_2d: Any,
      port: str = "i1",
  ) -> List[PipelineOutput]:
    """Buffered feed: numpy arrays → rtbot's ``receive_buffer`` with no
    row-by-row Message allocation. Operators that override
    ``receive_data_buffer`` (e.g. BurstAggregate) skip per-row boxing
    entirely; others fall back to the default per-row Message creation.
    Output is identical to :meth:`feed_batch`.
    """
    if not _HAS_NUMPY:
      raise ImportError("feed_buffer requires numpy")
    pipeline = self._pipelines[pipeline_id]
    native_outputs = pipeline.feed_buffer(
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

  def destroy(self, pipeline_id: str) -> None:
    self._pipelines.pop(pipeline_id, None)

  # -- Consolidated-session pipeline ----------------------------------

  def deploy_session(
      self, program_json: str, op_ids: List[str],
  ) -> "native.NativeSessionPipeline":
    """Instantiate a NativeSessionPipeline and register its output op ids.

    Unlike the regular per-view pipelines tracked by :meth:`deploy`, the
    session pipeline is returned directly — the caller (typically
    :class:`rtbot_sql.RtBotSql`) owns its lifetime.
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

  def _run_pipeline(self, pipeline: native.NativePipeline, input_messages: Iterable[Any]) -> List[PipelineOutput]:
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

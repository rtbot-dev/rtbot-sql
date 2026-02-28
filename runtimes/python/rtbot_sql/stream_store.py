"""In-memory stream message store."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


@dataclass
class Message:
  timestamp: int
  values: List[float]


def _coerce_message(item: Any) -> Message:
  if isinstance(item, Message):
    return Message(int(item.timestamp), [float(v) for v in item.values])
  if isinstance(item, dict):
    return Message(int(item["timestamp"]), [float(v) for v in item["values"]])
  if isinstance(item, (tuple, list)) and len(item) == 2:
    return Message(int(item[0]), [float(v) for v in item[1]])
  raise TypeError(f"Unsupported message payload: {type(item)!r}")


class InMemoryStreamStore:
  def __init__(self) -> None:
    self._streams: Dict[str, List[Message]] = {}

  def append(self, stream_name: str, timestamp: int, vector: List[float]) -> None:
    messages = self._streams.setdefault(stream_name, [])
    messages.append(Message(int(timestamp), [float(v) for v in vector]))

  def append_batch(self, stream_name: str, messages: Iterable[Any]) -> None:
    for item in messages:
      msg = _coerce_message(item)
      self.append(stream_name, msg.timestamp, msg.values)

  def read(self, stream_name: str) -> List[Message]:
    return list(self._streams.get(stream_name, []))

  def read_latest(self, stream_name: str, count: int) -> List[Message]:
    if count <= 0:
      return []
    data = self._streams.get(stream_name, [])
    if count >= len(data):
      return list(data)
    return list(data[-count:])

  def read_latest_before(self, stream_name: str, timestamp: int) -> Optional[Message]:
    data = self._streams.get(stream_name, [])
    target = int(timestamp)
    for msg in reversed(data):
      if msg.timestamp <= target:
        return Message(msg.timestamp, list(msg.values))
    return None

  def read_range(
      self,
      stream_name: str,
      from_time: Optional[int],
      to_time: Optional[int],
      limit: Optional[int] = None,
  ) -> List[Message]:
    result: List[Message] = []
    for msg in self._streams.get(stream_name, []):
      if from_time is not None and msg.timestamp < from_time:
        continue
      if to_time is not None and msg.timestamp > to_time:
        continue
      result.append(msg)
      if limit is not None and limit > 0 and len(result) >= limit:
        break
    return result

  def read_latest_per_key(
      self,
      view_name: str,
      known_keys: Iterable[float],
      key_index: int,
  ) -> Dict[float, Message]:
    wanted = {float(v) for v in known_keys}
    out: Dict[float, Message] = {}

    if not wanted:
      return out

    for msg in reversed(self._streams.get(view_name, [])):
      if key_index < 0 or key_index >= len(msg.values):
        continue
      key = float(msg.values[key_index])
      if key not in wanted or key in out:
        continue
      out[key] = msg
      if len(out) == len(wanted):
        break

    return out

  def clear(self, stream_name: str) -> None:
    self._streams.pop(stream_name, None)

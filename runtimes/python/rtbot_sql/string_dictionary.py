"""Bidirectional string <-> double dictionary for TEXT column encoding."""

from __future__ import annotations

from typing import Dict, Optional


class StringDictionary:
  """Encodes strings to sequential double IDs (starting at 1.0).

  Mirrors the Java StringDictionary and C++ StringDictionary:
  - encode(str) -> returns existing ID or assigns next sequential ID
  - decode(id) -> returns string or None
  - lookup(str) -> returns ID or None
  - put_mapping(id, str) -> direct insert (for syncing from C++ compiler)
  """

  def __init__(self) -> None:
    self._str_to_id: Dict[str, float] = {}
    self._id_to_str: Dict[float, str] = {}
    self._next_id: float = 1.0

  def encode(self, s: str) -> float:
    """Return existing ID for s, or assign the next sequential ID."""
    existing = self._str_to_id.get(s)
    if existing is not None:
      return existing
    fid = self._next_id
    self._next_id += 1.0
    self._str_to_id[s] = fid
    self._id_to_str[fid] = s
    return fid

  def decode(self, fid: float) -> Optional[str]:
    """Return the string for the given ID, or None."""
    return self._id_to_str.get(fid)

  def lookup(self, s: str) -> Optional[float]:
    """Return the ID for the given string, or None."""
    return self._str_to_id.get(s)

  def put_mapping(self, fid: float, s: str) -> None:
    """Directly insert a bidirectional mapping (idempotent).

    Used for syncing dictionary state from the C++ compiler.
    If the string or ID already exists, this is a no-op.
    """
    if s in self._str_to_id or fid in self._id_to_str:
      return
    self._str_to_id[s] = fid
    self._id_to_str[fid] = s
    if fid >= self._next_id:
      self._next_id = fid + 1.0

  @property
  def size(self) -> int:
    return len(self._str_to_id)

  @property
  def is_empty(self) -> bool:
    return len(self._str_to_id) == 0

  def all_entries(self) -> Dict[float, str]:
    """Return all ID -> string mappings."""
    return dict(self._id_to_str)

  def restore(self, serialized: Dict[str, str]) -> None:
    """Bulk restore from serialized state (string keys -> string values).

    Keys are string-ified doubles (e.g. "1", "2"). Clears existing state.
    """
    self._str_to_id.clear()
    self._id_to_str.clear()
    self._next_id = 1.0
    for id_str, s in serialized.items():
      fid = float(id_str)
      self._str_to_id[s] = fid
      self._id_to_str[fid] = s
      if fid >= self._next_id:
        self._next_id = fid + 1.0

"""Jupyter display helpers."""

from __future__ import annotations

import json
from typing import Dict


def _safe_node_id(node_id: str) -> str:
  return node_id.replace(":", "_").replace("-", "_")


def program_json_to_mermaid(program_json: str) -> str:
  parsed = json.loads(program_json)
  lines = ["flowchart LR"]

  operators = parsed.get("operators", [])
  for op in operators:
    op_id = str(op.get("id", "unknown"))
    op_type = str(op.get("type", "Operator"))
    lines.append(f'  {_safe_node_id(op_id)}["{op_id}\\n{op_type}"]')

  for connection in parsed.get("connections", []):
    src = _safe_node_id(str(connection.get("from", "")))
    dst = _safe_node_id(str(connection.get("to", "")))
    from_port = str(connection.get("fromPort", "o1"))
    to_port = str(connection.get("toPort", "i1"))
    lines.append(f'  {src} -->|"{from_port}->{to_port}"| {dst}')

  return "\n".join(lines)


def show_graph(program_json: str):
  mermaid = program_json_to_mermaid(program_json)

  try:
    from IPython.display import Markdown, display

    display(Markdown(f"```mermaid\n{mermaid}\n```"))
    return None
  except Exception:  # pragma: no cover - used outside notebooks
    return mermaid

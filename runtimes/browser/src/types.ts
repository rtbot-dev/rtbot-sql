// Types mirroring the C++ rtbot_sql::api types

export type ViewType = "SCALAR" | "KEYED" | "TOPK";

export type EntityType = "STREAM" | "VIEW" | "MATERIALIZED_VIEW" | "TABLE";

export type StatementType =
  | "CREATE_STREAM"
  | "CREATE_VIEW"
  | "CREATE_MATERIALIZED_VIEW"
  | "CREATE_TABLE"
  | "INSERT"
  | "SELECT"
  | "SUBSCRIBE"
  | "DROP";

export type SelectTier = "TIER1_READ" | "TIER2_SCAN" | "TIER3_EPHEMERAL";

export interface ColumnDef {
  name: string;
  index: number;
  type?: "DOUBLE" | "TEXT";
}

/**
 * The effective ingest mode for a `CREATE STREAM`. `scalar` is one
 * tag-value per event; `csv_burst` parses a comma-separated string of
 * N samples per event with the per-sample interval driven by
 * `window_clause.value` (Hz). When the SQL omits `TYPE`, the C++
 * compiler defaults to `scalar`.
 */
export type SourceType = "scalar" | "csv_burst";

/**
 * Span over the `TYPE` value identifier. Present only when the user
 * wrote `TYPE …` in the SQL — absent when the type defaults to scalar.
 */
export interface SourceTypeClause {
  value: SourceType;
  line: number;
  column: number;
  end_line: number;
  end_column: number;
}

/**
 * Span over the `WINDOW` digits. Only valid (and required) with
 * `TYPE csv_burst`; the analyzer flags the clause otherwise.
 */
export interface SourceWindowClause {
  value: number;
  line: number;
  column: number;
  end_line: number;
  end_column: number;
}

/**
 * Metadata from `FROM "<pattern>"` on a `CREATE STREAM`. `name` is the
 * unquoted pattern verbatim (e.g. `"ignition://{x}/cpu"`); `{col}`
 * placeholders reference TEXT columns in the schema by name and are
 * resolved at deploy time against the gateway tag tree.
 *
 * `type` reflects the effective type (defaults to "scalar" when the
 * SQL omits `TYPE`). The optional `type_clause` / `window_clause`
 * carry the user-written tokens with their spans, for editor
 * diagnostics on `WINDOW > 0`, unknown TYPE, etc.
 */
export interface Source {
  name: string;
  type: SourceType;
  line: number;
  column: number;
  end_line: number;
  end_column: number;
  type_clause?: SourceTypeClause;
  window_clause?: SourceWindowClause;
}

export interface StreamSchema {
  name: string;
  columns: ColumnDef[];
  source?: Source;
}

export interface ViewMeta {
  name: string;
  entity_type: EntityType;
  view_type: ViewType;
  field_map: Record<string, number>;
  source_streams: string[];
  program_json: string;
  output_stream: string;
  per_key_prefix: string;
  known_keys: number[];
  key_index: number;
}

export interface TableSchema {
  name: string;
  columns: ColumnDef[];
  changelog_stream: string;
  key_columns: number[];
}

export interface CatalogSnapshot {
  streams: Record<string, StreamSchema>;
  views: Record<string, ViewMeta>;
  tables: Record<string, TableSchema>;
  dictionaries?: Record<string, Record<string, string>>;
}

export interface CompilationError {
  message: string;
  line: number;
  column: number;
  end_line: number;
  end_column: number;
}

export interface CompilationResult {
  statement_type: StatementType;
  entity_name: string;
  program_json: string;
  field_map: Record<string, number>;
  source_streams: string[];
  view_type: ViewType;
  key_index: number;
  select_tier: SelectTier;
  insert_payload: number[];
  stream_schema: StreamSchema;
  table_schema: TableSchema;
  drop_entity_name: string;
  drop_entity_type: EntityType;
  errors: CompilationError[];
  dictionary_updates?: Record<string, Record<string, string>>;
}

export interface ExpandedCompilationResult {
  results: CompilationResult[];
  new_ts_units_per_second: number;
}

export interface ValidationResult {
  valid: boolean;
  errors: CompilationError[];
}

export interface Message {
  timestamp: number;
  values: number[];
}

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
}

export interface StreamSchema {
  name: string;
  columns: ColumnDef[];
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
}

export interface CompilationError {
  message: string;
  line: number;
  column: number;
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
}

export interface ValidationResult {
  valid: boolean;
  errors: CompilationError[];
}

export interface Message {
  timestamp: number;
  values: number[];
}

import { InMemoryCatalog } from "./catalog";
import { InMemoryStreamStore, WatchCallback } from "./stream_store";
import { StubPipelineRunner, PipelineRunner } from "./pipeline_runner";
import { formatRow, FormattedRow } from "./formatter";
import {
  CatalogSnapshot,
  CompilationResult,
  ValidationResult,
  Message,
} from "./types";
import type { RtBotSqlWasmModule } from "./rtbot_sql_wasm";

export {
  InMemoryCatalog,
  InMemoryStreamStore,
  StubPipelineRunner,
  formatRow,
};
export type {
  PipelineRunner,
  FormattedRow,
  WatchCallback,
  RtBotSqlWasmModule,
};
export * from "./types";

export interface RtBotSqlOptions {
  wasmModule: RtBotSqlWasmModule;
  runner?: PipelineRunner;
}

export class RtBotSql {
  private wasm: RtBotSqlWasmModule;
  private catalog: InMemoryCatalog;
  private store: InMemoryStreamStore;
  private runner: PipelineRunner;

  constructor(opts: RtBotSqlOptions) {
    this.wasm = opts.wasmModule;
    this.catalog = new InMemoryCatalog();
    this.store = new InMemoryStreamStore();
    this.runner = opts.runner ?? new StubPipelineRunner();
  }

  execute(sql: string): CompilationResult {
    const result = RtBotSql.compile(sql, this.catalog.snapshot(), this.wasm);
    if (result.errors.length > 0) return result;

    switch (result.statement_type) {
      case "CREATE_STREAM":
        this.catalog.registerStream(
          result.entity_name,
          result.stream_schema,
        );
        break;

      case "CREATE_VIEW":
      case "CREATE_MATERIALIZED_VIEW":
        this.catalog.registerView(result.entity_name, {
          name: result.entity_name,
          entity_type:
            result.statement_type === "CREATE_VIEW"
              ? "VIEW"
              : "MATERIALIZED_VIEW",
          view_type: result.view_type,
          field_map: result.field_map,
          source_streams: result.source_streams,
          program_json: result.program_json,
          output_stream: "",
          per_key_prefix: "",
          known_keys: [],
          key_index: result.key_index,
        });
        break;

      case "CREATE_TABLE":
        this.catalog.registerTable(result.entity_name, result.table_schema);
        break;

      case "INSERT": {
        const msg: Message = {
          timestamp: Date.now(),
          values: result.insert_payload,
        };
        this.store.append(result.entity_name, msg);
        break;
      }

      case "DROP":
        this.catalog.drop(result.drop_entity_name);
        break;

      case "SELECT":
        // SELECT results are in the compilation result
        break;
    }

    return result;
  }

  subscribe(
    streamName: string,
    callback: WatchCallback,
  ): () => void {
    return this.store.watch(streamName, callback);
  }

  getCatalog(): InMemoryCatalog {
    return this.catalog;
  }

  getStore(): InMemoryStreamStore {
    return this.store;
  }

  static compile(
    sql: string,
    catalog: CatalogSnapshot,
    wasm: RtBotSqlWasmModule,
  ): CompilationResult {
    const resultJson = wasm.compileSqlJson(sql, JSON.stringify(catalog));
    return JSON.parse(resultJson) as CompilationResult;
  }

  static validate(sql: string, wasm: RtBotSqlWasmModule): ValidationResult {
    const resultJson = wasm.validateSql(sql);
    return JSON.parse(resultJson) as ValidationResult;
  }
}

import { InMemoryCatalog } from "./catalog";
import { InMemoryStreamStore, WatchCallback } from "./stream_store";
import { StubPipelineRunner, PipelineRunner } from "./pipeline_runner";
import { formatRow, FormattedRow } from "./formatter";
import { StringDictionary } from "./string_dictionary";
import {
  CatalogSnapshot,
  CompilationResult,
  ExpandedCompilationResult,
  ValidationResult,
  Message,
} from "./types";
import type { RtBotSqlWasmModule } from "./rtbot_sql_wasm";

export {
  InMemoryCatalog,
  InMemoryStreamStore,
  StubPipelineRunner,
  formatRow,
  StringDictionary,
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
  private tsUnitsPerSecond: number = 1000;

  constructor(opts: RtBotSqlOptions) {
    this.wasm = opts.wasmModule;
    this.catalog = new InMemoryCatalog();
    this.store = new InMemoryStreamStore();
    this.runner = opts.runner ?? new StubPipelineRunner();
  }

  execute(sql: string): CompilationResult {
    const expanded = RtBotSql.compile(sql, this.catalog.snapshot(), this.wasm, this.tsUnitsPerSecond);

    if (expanded.new_ts_units_per_second > 0) {
      this.tsUnitsPerSecond = expanded.new_ts_units_per_second;
      return {
        statement_type: "CREATE_STREAM",
        entity_name: "",
        program_json: "",
        field_map: {},
        source_streams: [],
        view_type: "SCALAR",
        key_index: -1,
        select_tier: "TIER1_READ",
        insert_payload: [],
        stream_schema: { name: "", columns: [] },
        table_schema: { name: "", columns: [], changelog_stream: "", key_columns: [] },
        drop_entity_name: "",
        drop_entity_type: "STREAM",
        errors: [],
      };
    }

    let lastResult: CompilationResult | undefined;

    for (const result of expanded.results) {
      lastResult = result;
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
          // Sync dictionary updates from C++ compiler
          if (result.dictionary_updates) {
            for (const [dictKey, entries] of Object.entries(result.dictionary_updates)) {
              const dict = this.catalog.getOrCreateDictionary(dictKey);
              for (const [idStr, str] of Object.entries(entries)) {
                dict.putMapping(Number(idStr), str);
              }
            }
          }

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
    }

    // Return last result, or a default empty result if there were no results
    return lastResult ?? {
      statement_type: "CREATE_STREAM",
      entity_name: "",
      program_json: "",
      field_map: {},
      source_streams: [],
      view_type: "SCALAR",
      key_index: -1,
      select_tier: "TIER1_READ",
      insert_payload: [],
      stream_schema: { name: "", columns: [] },
      table_schema: { name: "", columns: [], changelog_stream: "", key_columns: [] },
      drop_entity_name: "",
      drop_entity_type: "STREAM",
      errors: [],
    };
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

  insertMixed(streamName: string, timestamp: number, values: (number | string)[]): void {
    const schema = this.catalog.lookupStream(streamName);
    if (!schema) throw new Error(`unknown stream: ${streamName}`);
    if (values.length !== schema.columns.length) {
      throw new Error(
        `expected ${schema.columns.length} values, got ${values.length}`,
      );
    }

    const encoded: number[] = [];
    for (let i = 0; i < values.length; i++) {
      const col = schema.columns[i];
      const val = values[i];

      if (col.type === "TEXT") {
        if (typeof val !== "string") {
          throw new Error(
            `expected string for TEXT column '${col.name}', got ${typeof val}`,
          );
        }
        const dictKey = `${streamName}.${col.name}`;
        const dict = this.catalog.getOrCreateDictionary(dictKey);
        encoded.push(dict.encode(val));
      } else {
        if (typeof val === "string") {
          throw new Error(
            `expected number for DOUBLE column '${col.name}', got string`,
          );
        }
        encoded.push(val);
      }
    }

    this.store.append(streamName, { timestamp, values: encoded });
  }

  decodeRow(streamName: string, values: number[]): (number | string)[] {
    const schema = this.catalog.lookupStream(streamName);
    if (!schema) return [...values];

    return values.map((val, i) => {
      if (i < schema.columns.length && schema.columns[i].type === "TEXT") {
        const dictKey = `${streamName}.${schema.columns[i].name}`;
        const dict = this.catalog.lookupDictionary(dictKey);
        if (dict) {
          const s = dict.decode(val);
          return s !== undefined ? s : val;
        }
      }
      return val;
    });
  }

  static compile(
    sql: string,
    catalog: CatalogSnapshot,
    wasm: RtBotSqlWasmModule,
    tsUnitsPerSecond: number = 1000,
  ): ExpandedCompilationResult {
    const resultJson = wasm.compileSqlJson(sql, JSON.stringify(catalog), tsUnitsPerSecond);
    return JSON.parse(resultJson) as ExpandedCompilationResult;
  }

  static validate(sql: string, wasm: RtBotSqlWasmModule): ValidationResult {
    const resultJson = wasm.validateSql(sql);
    return JSON.parse(resultJson) as ValidationResult;
  }
}

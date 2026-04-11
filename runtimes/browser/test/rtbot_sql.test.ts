import {
  InMemoryCatalog,
  InMemoryStreamStore,
  RtBotSql,
  formatRow,
  StubPipelineRunner,
  StringDictionary,
} from "../index";
import type {
  RtBotSqlWasmModule,
  CompilationResult,
  ExpandedCompilationResult,
  ValidationResult,
  CatalogSnapshot,
  ColumnDef,
  Message,
} from "../index";

// --- Mock WASM module for unit tests ---

function wrapExpanded(result: CompilationResult): string {
  const expanded: ExpandedCompilationResult = {
    results: [result],
    new_ts_units_per_second: -1,
  };
  return JSON.stringify(expanded);
}

function wrapExpandedErrors(errors: { message: string; line: number; column: number }[]): string {
  const result: CompilationResult = {
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
    errors,
  };
  return wrapExpanded(result);
}

function createMockWasm(): RtBotSqlWasmModule {
  return {
    compileSqlJson(sql: string, catalogJson: string, _tsUnitsPerSecond?: number): string {
      const catalog: CatalogSnapshot = JSON.parse(catalogJson);

      // Simple mock: parse CREATE STREAM
      const createMatch = sql.match(
        /CREATE\s+STREAM\s+(\w+)\s*\(([^)]+)\)/i,
      );
      if (createMatch) {
        const name = createMatch[1];
        const colDefs = createMatch[2].split(",").map((c, i) => ({
          name: c.trim().split(/\s+/)[0],
          index: i,
        }));
        const result: CompilationResult = {
          statement_type: "CREATE_STREAM",
          entity_name: name,
          program_json: "",
          field_map: {},
          source_streams: [],
          view_type: "SCALAR",
          key_index: -1,
          select_tier: "TIER1_READ",
          insert_payload: [],
          stream_schema: { name, columns: colDefs },
          table_schema: {
            name: "",
            columns: [],
            changelog_stream: "",
            key_columns: [],
          },
          drop_entity_name: "",
          drop_entity_type: "STREAM",
          errors: [],
        };
        return wrapExpanded(result);
      }

      // Simple mock: parse INSERT
      const insertMatch = sql.match(
        /INSERT\s+INTO\s+(\w+).*VALUES\s*\(([^)]+)\)/i,
      );
      if (insertMatch) {
        const name = insertMatch[1];
        const values = insertMatch[2].split(",").map((v) => parseFloat(v.trim()));
        const result: CompilationResult = {
          statement_type: "INSERT",
          entity_name: name,
          program_json: "",
          field_map: {},
          source_streams: [],
          view_type: "SCALAR",
          key_index: -1,
          select_tier: "TIER1_READ",
          insert_payload: values,
          stream_schema: { name: "", columns: [] },
          table_schema: {
            name: "",
            columns: [],
            changelog_stream: "",
            key_columns: [],
          },
          drop_entity_name: "",
          drop_entity_type: "STREAM",
          errors: [],
        };
        return wrapExpanded(result);
      }

      // Simple mock: parse SELECT
      const selectMatch = sql.match(/SELECT\s+(.+)\s+FROM\s+(\w+)/i);
      if (selectMatch) {
        const source = selectMatch[2];
        if (!catalog.streams[source] && !catalog.views[source]) {
          return wrapExpandedErrors([
            { message: `unknown source: ${source}`, line: -1, column: -1 },
          ]);
        }
        const result: CompilationResult = {
          statement_type: "SELECT",
          entity_name: "",
          program_json: '{"operators":[],"connections":[]}',
          field_map: { value: 0 },
          source_streams: [source],
          view_type: "SCALAR",
          key_index: -1,
          select_tier: "TIER3_EPHEMERAL",
          insert_payload: [],
          stream_schema: { name: "", columns: [] },
          table_schema: {
            name: "",
            columns: [],
            changelog_stream: "",
            key_columns: [],
          },
          drop_entity_name: "",
          drop_entity_type: "STREAM",
          errors: [],
        };
        return wrapExpanded(result);
      }

      return wrapExpandedErrors([
        { message: "unsupported statement type", line: -1, column: -1 },
      ]);
    },

    validateSql(sql: string): string {
      // Simple validation: check for basic SQL keyword
      const valid = /^\s*(SELECT|CREATE|INSERT|DROP)\s/i.test(sql);
      const result: ValidationResult = {
        valid,
        errors: valid
          ? []
          : [{ message: "syntax error", line: -1, column: -1 }],
      };
      return JSON.stringify(result);
    },
  };
}

// --- InMemoryCatalog tests ---

describe("InMemoryCatalog", () => {
  it("registers and looks up a stream", () => {
    const catalog = new InMemoryCatalog();
    catalog.registerStream("prices", {
      name: "prices",
      columns: [
        { name: "price", index: 0 },
        { name: "volume", index: 1 },
      ],
    });

    const schema = catalog.lookupStream("prices");
    expect(schema).toBeDefined();
    expect(schema!.columns).toHaveLength(2);
    expect(schema!.columns[0].name).toBe("price");
  });

  it("produces a snapshot", () => {
    const catalog = new InMemoryCatalog();
    catalog.registerStream("s1", { name: "s1", columns: [] });
    catalog.registerStream("s2", {
      name: "s2",
      columns: [{ name: "x", index: 0 }],
    });

    const snap = catalog.snapshot();
    expect(Object.keys(snap.streams)).toEqual(["s1", "s2"]);
    expect(snap.streams["s2"].columns[0].name).toBe("x");
  });

  it("drops an entity", () => {
    const catalog = new InMemoryCatalog();
    catalog.registerStream("s1", { name: "s1", columns: [] });
    expect(catalog.drop("s1")).toBe(true);
    expect(catalog.lookupStream("s1")).toBeUndefined();
    expect(catalog.drop("nonexistent")).toBe(false);
  });
});

// --- InMemoryStreamStore tests ---

describe("InMemoryStreamStore", () => {
  it("appends and reads messages", () => {
    const store = new InMemoryStreamStore();
    store.append("prices", { timestamp: 1, values: [100] });
    store.append("prices", { timestamp: 2, values: [101] });

    const msgs = store.read("prices");
    expect(msgs).toHaveLength(2);
    expect(msgs[0].values[0]).toBe(100);
  });

  it("returns empty array for unknown stream", () => {
    const store = new InMemoryStreamStore();
    expect(store.read("unknown")).toEqual([]);
  });

  it("watch receives new messages", () => {
    const store = new InMemoryStreamStore();
    const received: Message[] = [];

    const unsub = store.watch("prices", (msg) => received.push(msg));
    store.append("prices", { timestamp: 1, values: [50] });
    store.append("prices", { timestamp: 2, values: [51] });

    expect(received).toHaveLength(2);

    unsub();
    store.append("prices", { timestamp: 3, values: [52] });
    expect(received).toHaveLength(2); // no new callback after unsub
  });
});

// --- Formatter tests ---

describe("formatRow", () => {
  it("maps vector indices to named fields", () => {
    const msg: Message = { timestamp: 100, values: [1.5, 2.5, 3.5] };
    const row = formatRow(msg, { price: 0, volume: 2 });
    expect(row.timestamp).toBe(100);
    expect(row.price).toBe(1.5);
    expect(row.volume).toBe(3.5);
  });
});

// --- StubPipelineRunner tests ---

describe("StubPipelineRunner", () => {
  it("throws on all operations", () => {
    const runner = new StubPipelineRunner();
    expect(() => runner.deploy("{}", "p1")).toThrow();
    expect(() => runner.feed("p1", 0, [])).toThrow();
    expect(() => runner.runOnce("p1", 0, [])).toThrow();
    expect(() => runner.destroy("p1")).toThrow();
  });
});

// --- Validation tests (via mock) ---

describe("RtBotSql.validate", () => {
  const wasm = createMockWasm();

  it("accepts valid SQL", () => {
    const result = RtBotSql.validate("SELECT x FROM stream1", wasm);
    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
  });

  it("rejects invalid SQL", () => {
    const result = RtBotSql.validate("INVALID GIBBERISH", wasm);
    expect(result.valid).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
  });
});

// --- Compilation tests (via mock) ---

describe("RtBotSql.compile", () => {
  const wasm = createMockWasm();

  it("compiles a SELECT with catalog", () => {
    const catalog: CatalogSnapshot = {
      streams: {
        prices: {
          name: "prices",
          columns: [{ name: "price", index: 0 }],
        },
      },
      views: {},
      tables: {},
    };

    const expanded = RtBotSql.compile("SELECT price FROM prices", catalog, wasm);
    expect(expanded.results).toHaveLength(1);
    expect(expanded.new_ts_units_per_second).toBe(-1);
    const result = expanded.results[0];
    expect(result.errors).toHaveLength(0);
    expect(result.statement_type).toBe("SELECT");
    expect(result.source_streams).toContain("prices");
  });

  it("returns error for unknown source", () => {
    const catalog: CatalogSnapshot = {
      streams: {},
      views: {},
      tables: {},
    };

    const expanded = RtBotSql.compile(
      "SELECT x FROM nonexistent",
      catalog,
      wasm,
    );
    expect(expanded.results[0].errors.length).toBeGreaterThan(0);
  });
});

// --- Full lifecycle test (via mock) ---

describe("RtBotSql lifecycle", () => {
  it("CREATE STREAM -> INSERT -> SELECT", () => {
    const wasm = createMockWasm();
    const rtbotSql = new RtBotSql({ wasmModule: wasm });

    // CREATE STREAM
    const createResult = rtbotSql.execute(
      "CREATE STREAM prices (price DOUBLE, volume DOUBLE)",
    );
    expect(createResult.errors).toHaveLength(0);
    expect(createResult.statement_type).toBe("CREATE_STREAM");

    // Verify stream was registered
    const schema = rtbotSql.getCatalog().lookupStream("prices");
    expect(schema).toBeDefined();
    expect(schema!.columns).toHaveLength(2);

    // INSERT
    const insertResult = rtbotSql.execute(
      "INSERT INTO prices VALUES (100.5, 200)",
    );
    expect(insertResult.errors).toHaveLength(0);
    expect(insertResult.statement_type).toBe("INSERT");

    // Verify message stored
    const messages = rtbotSql.getStore().read("prices");
    expect(messages).toHaveLength(1);
    expect(messages[0].values).toEqual([100.5, 200]);

    // SELECT
    const selectResult = rtbotSql.execute("SELECT price FROM prices");
    expect(selectResult.errors).toHaveLength(0);
    expect(selectResult.statement_type).toBe("SELECT");
  });

  it("subscribe receives new inserts", () => {
    const wasm = createMockWasm();
    const rtbotSql = new RtBotSql({ wasmModule: wasm });

    rtbotSql.execute("CREATE STREAM ticks (value DOUBLE)");

    const received: Message[] = [];
    const unsub = rtbotSql.subscribe("ticks", (msg) => received.push(msg));

    rtbotSql.execute("INSERT INTO ticks VALUES (42)");
    rtbotSql.execute("INSERT INTO ticks VALUES (43)");

    expect(received).toHaveLength(2);
    expect(received[0].values).toEqual([42]);
    expect(received[1].values).toEqual([43]);

    unsub();
  });
});

// --- Task 1: TypeScript type updates for dictionaries ---

describe("TypeScript type updates for dictionaries", () => {
  it("ColumnDef has optional type field", () => {
    const col: ColumnDef = { name: "loc", index: 0, type: "TEXT" };
    expect(col.type).toBe("TEXT");
  });

  it("ColumnDef type defaults to undefined (backward compat)", () => {
    const col: ColumnDef = { name: "price", index: 0 };
    expect(col.type).toBeUndefined();
  });

  it("CatalogSnapshot has optional dictionaries field", () => {
    const snap: CatalogSnapshot = {
      streams: {},
      views: {},
      tables: {},
      dictionaries: { "s.col": { "1": "Bay A" } },
    };
    expect(snap.dictionaries!["s.col"]["1"]).toBe("Bay A");
  });

  it("CompilationResult has optional dictionary_updates field", () => {
    const result: CompilationResult = {
      statement_type: "INSERT",
      entity_name: "s",
      program_json: "",
      field_map: {},
      source_streams: [],
      view_type: "SCALAR",
      key_index: -1,
      select_tier: "TIER1_READ",
      insert_payload: [1.0],
      stream_schema: { name: "", columns: [] },
      table_schema: { name: "", columns: [], changelog_stream: "", key_columns: [] },
      drop_entity_name: "",
      drop_entity_type: "STREAM",
      errors: [],
      dictionary_updates: { "s.loc": { "1": "Bay A" } },
    };
    expect(result.dictionary_updates!["s.loc"]["1"]).toBe("Bay A");
  });
});

// --- Task 2: StringDictionary ---

describe("StringDictionary", () => {
  it("encode assigns sequential IDs starting at 1", () => {
    const d = new StringDictionary();
    expect(d.encode("alpha")).toBe(1);
    expect(d.encode("beta")).toBe(2);
    expect(d.encode("alpha")).toBe(1);
  });

  it("decode roundtrip", () => {
    const d = new StringDictionary();
    d.encode("hello");
    expect(d.decode(1)).toBe("hello");
    expect(d.decode(99)).toBeUndefined();
  });

  it("lookup", () => {
    const d = new StringDictionary();
    d.encode("hello");
    expect(d.lookup("hello")).toBe(1);
    expect(d.lookup("unknown")).toBeUndefined();
  });

  it("putMapping adds new entry", () => {
    const d = new StringDictionary();
    d.putMapping(5, "hello");
    expect(d.decode(5)).toBe("hello");
    expect(d.lookup("hello")).toBe(5);
    expect(d.encode("world")).toBe(6);
  });

  it("putMapping is idempotent for existing string", () => {
    const d = new StringDictionary();
    d.encode("hello");
    d.putMapping(99, "hello");
    expect(d.lookup("hello")).toBe(1);
    expect(d.decode(99)).toBeUndefined();
  });

  it("putMapping is idempotent for existing ID", () => {
    const d = new StringDictionary();
    d.putMapping(1, "hello");
    d.putMapping(1, "world");
    expect(d.decode(1)).toBe("hello");
    expect(d.lookup("world")).toBeUndefined();
  });

  it("size and isEmpty", () => {
    const d = new StringDictionary();
    expect(d.isEmpty).toBe(true);
    expect(d.size).toBe(0);
    d.encode("a");
    expect(d.isEmpty).toBe(false);
    expect(d.size).toBe(1);
  });

  it("allEntries returns all mappings", () => {
    const d = new StringDictionary();
    d.encode("alpha");
    d.encode("beta");
    expect(d.allEntries()).toEqual(new Map([[1, "alpha"], [2, "beta"]]));
  });

  it("restore from serialized state", () => {
    const d = new StringDictionary();
    d.restore({ "1": "alpha", "2": "beta" });
    expect(d.decode(1)).toBe("alpha");
    expect(d.decode(2)).toBe("beta");
    expect(d.encode("gamma")).toBe(3);
  });
});

// --- Task 3: InMemoryCatalog dictionary support ---

describe("InMemoryCatalog dictionary support", () => {
  it("getOrCreateDictionary returns same instance", () => {
    const catalog = new InMemoryCatalog();
    const d1 = catalog.getOrCreateDictionary("s.col");
    const d2 = catalog.getOrCreateDictionary("s.col");
    expect(d1).toBe(d2);
  });

  it("lookupDictionary returns undefined for unknown", () => {
    const catalog = new InMemoryCatalog();
    expect(catalog.lookupDictionary("unknown")).toBeUndefined();
  });

  it("snapshot includes dictionaries", () => {
    const catalog = new InMemoryCatalog();
    const d = catalog.getOrCreateDictionary("s.col");
    d.encode("Bay A");
    const snap = catalog.snapshot();
    expect(snap.dictionaries).toBeDefined();
    expect(snap.dictionaries!["s.col"]["1"]).toBe("Bay A");
  });

  it("drop clears related dictionaries", () => {
    const catalog = new InMemoryCatalog();
    catalog.registerStream("s", { name: "s", columns: [] });
    catalog.getOrCreateDictionary("s.col").encode("Bay A");
    catalog.drop("s");
    expect(catalog.lookupDictionary("s.col")).toBeUndefined();
  });
});

// --- Task 4: RtBotSql insertMixed and decodeRow ---

describe("RtBotSql insertMixed and decodeRow", () => {
  it("insertMixed encodes strings to dictionary IDs", () => {
    const wasm = createMockWasm();
    const rtbotSql = new RtBotSql({ wasmModule: wasm });
    rtbotSql.getCatalog().registerStream("sensors", {
      name: "sensors",
      columns: [
        { name: "device_id", index: 0, type: "DOUBLE" },
        { name: "location", index: 1, type: "TEXT" },
        { name: "value", index: 2, type: "DOUBLE" },
      ],
    });

    rtbotSql.insertMixed("sensors", 1000, [1.0, "Bay A", 42.0]);
    const msgs = rtbotSql.getStore().read("sensors");
    expect(msgs).toHaveLength(1);
    expect(msgs[0].values).toEqual([1.0, 1, 42.0]);
  });

  it("insertMixed reuses existing dictionary IDs", () => {
    const wasm = createMockWasm();
    const rtbotSql = new RtBotSql({ wasmModule: wasm });
    rtbotSql.getCatalog().registerStream("sensors", {
      name: "sensors",
      columns: [
        { name: "device_id", index: 0, type: "DOUBLE" },
        { name: "location", index: 1, type: "TEXT" },
        { name: "value", index: 2, type: "DOUBLE" },
      ],
    });

    rtbotSql.insertMixed("sensors", 1000, [1.0, "Bay A", 42.0]);
    rtbotSql.insertMixed("sensors", 2000, [2.0, "Bay B", 99.0]);
    rtbotSql.insertMixed("sensors", 3000, [3.0, "Bay A", 55.0]);

    const msgs = rtbotSql.getStore().read("sensors");
    expect(msgs[0].values[1]).toBe(1);
    expect(msgs[1].values[1]).toBe(2);
    expect(msgs[2].values[1]).toBe(1);
  });

  it("decodeRow converts TEXT column IDs back to strings", () => {
    const wasm = createMockWasm();
    const rtbotSql = new RtBotSql({ wasmModule: wasm });
    rtbotSql.getCatalog().registerStream("sensors", {
      name: "sensors",
      columns: [
        { name: "device_id", index: 0, type: "DOUBLE" },
        { name: "location", index: 1, type: "TEXT" },
        { name: "value", index: 2, type: "DOUBLE" },
      ],
    });

    rtbotSql.insertMixed("sensors", 1000, [1.0, "Bay A", 42.0]);
    rtbotSql.insertMixed("sensors", 2000, [2.0, "Bay B", 99.0]);

    const msgs = rtbotSql.getStore().read("sensors");
    expect(rtbotSql.decodeRow("sensors", msgs[0].values)).toEqual([1.0, "Bay A", 42.0]);
    expect(rtbotSql.decodeRow("sensors", msgs[1].values)).toEqual([2.0, "Bay B", 99.0]);
  });

  it("decodeRow for unknown stream returns raw values", () => {
    const wasm = createMockWasm();
    const rtbotSql = new RtBotSql({ wasmModule: wasm });
    expect(rtbotSql.decodeRow("unknown", [1.0, 2.0])).toEqual([1.0, 2.0]);
  });

  it("insertMixed throws for string in DOUBLE column", () => {
    const wasm = createMockWasm();
    const rtbotSql = new RtBotSql({ wasmModule: wasm });
    rtbotSql.getCatalog().registerStream("sensors", {
      name: "sensors",
      columns: [
        { name: "device_id", index: 0, type: "DOUBLE" },
        { name: "location", index: 1, type: "TEXT" },
        { name: "value", index: 2, type: "DOUBLE" },
      ],
    });
    expect(() =>
      rtbotSql.insertMixed("sensors", 1000, [1.0, "Bay A", "not a number"]),
    ).toThrow();
  });

  it("insertMixed throws for number in TEXT column", () => {
    const wasm = createMockWasm();
    const rtbotSql = new RtBotSql({ wasmModule: wasm });
    rtbotSql.getCatalog().registerStream("sensors", {
      name: "sensors",
      columns: [
        { name: "device_id", index: 0, type: "DOUBLE" },
        { name: "location", index: 1, type: "TEXT" },
        { name: "value", index: 2, type: "DOUBLE" },
      ],
    });
    expect(() =>
      rtbotSql.insertMixed("sensors", 1000, [1.0, 42.0, 42.0]),
    ).toThrow();
  });
});

// --- Task 5: SQL INSERT dictionary sync ---

describe("RtBotSql SQL INSERT dictionary sync", () => {
  it("syncs dictionary_updates from compilation result", () => {
    const mockWasm: RtBotSqlWasmModule = {
      compileSqlJson(sql: string, _catalogJson: string, _tsUnitsPerSecond?: number): string {
        const createMatch = sql.match(/CREATE\s+STREAM\s+(\w+)\s*\(([^)]+)\)/i);
        if (createMatch) {
          const name = createMatch[1];
          const colDefs = createMatch[2].split(",").map((c, i) => {
            const parts = c.trim().split(/\s+/);
            const colType = parts.slice(1).join(" ").toUpperCase().includes("TEXT") ? "TEXT" : "DOUBLE";
            return { name: parts[0], index: i, type: colType };
          });
          return wrapExpanded({
            statement_type: "CREATE_STREAM",
            entity_name: name,
            stream_schema: { name, columns: colDefs },
            errors: [],
            program_json: "", field_map: {}, source_streams: [],
            view_type: "SCALAR", key_index: -1, select_tier: "TIER1_READ",
            insert_payload: [],
            table_schema: { name: "", columns: [], changelog_stream: "", key_columns: [] },
            drop_entity_name: "", drop_entity_type: "STREAM",
          } as CompilationResult);
        }

        const insertMatch = sql.match(/INSERT/i);
        if (insertMatch) {
          return wrapExpanded({
            statement_type: "INSERT",
            entity_name: "sensors",
            insert_payload: [1.0, 1.0, 42.0],
            dictionary_updates: { "sensors.location": { "1": "Bay A" } },
            errors: [],
            program_json: "", field_map: {}, source_streams: [],
            view_type: "SCALAR", key_index: -1, select_tier: "TIER1_READ",
            stream_schema: { name: "", columns: [] },
            table_schema: { name: "", columns: [], changelog_stream: "", key_columns: [] },
            drop_entity_name: "", drop_entity_type: "STREAM",
          } as CompilationResult);
        }

        return wrapExpandedErrors([{ message: "unsupported", line: -1, column: -1 }]);
      },
      validateSql: () => JSON.stringify({ valid: true, errors: [] }),
    };

    const rtbotSql = new RtBotSql({ wasmModule: mockWasm });
    rtbotSql.execute("CREATE STREAM sensors (device_id DOUBLE, location TEXT, value DOUBLE)");
    rtbotSql.execute("INSERT INTO sensors VALUES (1, 'Bay A', 42.0)");

    const dict = rtbotSql.getCatalog().lookupDictionary("sensors.location");
    expect(dict).toBeDefined();
    expect(dict!.decode(1)).toBe("Bay A");

    const msgs = rtbotSql.getStore().read("sensors");
    expect(rtbotSql.decodeRow("sensors", msgs[0].values)).toEqual([1.0, "Bay A", 42.0]);
  });
});

// --- Task 3 (browser): CREATE ALIGNED STREAM ... BIN() sugar syntax catalog ---

describe("CREATE ALIGNED STREAM BIN() sugar syntax catalog", () => {
  // Pipeline: vibration → vibration_bin → vibration_rs → input
  //
  // In a real runtime, data would flow as:
  //   vibration:     (t=50ms, [8.0]), (t=200ms, [12.0]), ... → AVG = 10.0
  //   vibration_bin: (t=1050ms, [10.0])  ← bin 0 avg flushed
  //   vibration_rs:  (t=1000ms, [10.0])  ← TIMESHIFT(-1000) corrected
  //   input:         (t=1000ms, [10.0, 75.0])  ← cross-join
  //
  // This test verifies catalog population only (StubPipelineRunner — no data flow).

  /** Build a default CompilationResult with all required fields. */
  function baseResult(overrides: Partial<CompilationResult>): CompilationResult {
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
      ...overrides,
    };
  }

  it("aligned stream sugar populates catalog with streams and views", () => {
    const alignedSql = "CREATE ALIGNED STREAM input (vibration DOUBLE, bearing_temp DOUBLE) BIN(1s)";

    const mockWasm: RtBotSqlWasmModule = {
      compileSqlJson(sql: string, _catalogJson: string, _tsUnitsPerSecond?: number): string {
        if (sql === alignedSql) {
          const expanded: ExpandedCompilationResult = {
            results: [
              // Result 1: CREATE_STREAM for "vibration"
              baseResult({
                statement_type: "CREATE_STREAM",
                entity_name: "vibration",
                stream_schema: {
                  name: "vibration",
                  columns: [{ name: "value", index: 0 }],
                },
              }),
              // Result 2: CREATE_STREAM for "bearing_temp"
              baseResult({
                statement_type: "CREATE_STREAM",
                entity_name: "bearing_temp",
                stream_schema: {
                  name: "bearing_temp",
                  columns: [{ name: "value", index: 0 }],
                },
              }),
              // Result 3: CREATE_VIEW for "vibration_bin"
              baseResult({
                statement_type: "CREATE_VIEW",
                entity_name: "vibration_bin",
                source_streams: ["vibration"],
                field_map: { vibration: 0 },
                program_json: '{"operators":[{"type":"BIN_AVG"}],"connections":[]}',
              }),
              // Result 4: CREATE_VIEW for "bearing_temp_bin"
              baseResult({
                statement_type: "CREATE_VIEW",
                entity_name: "bearing_temp_bin",
                source_streams: ["bearing_temp"],
                field_map: { bearing_temp: 0 },
                program_json: '{"operators":[{"type":"BIN_AVG"}],"connections":[]}',
              }),
              // Result 5: CREATE_VIEW for "vibration_rs"
              baseResult({
                statement_type: "CREATE_VIEW",
                entity_name: "vibration_rs",
                source_streams: ["vibration_bin"],
                field_map: { vibration: 0 },
                program_json: '{"operators":[{"type":"TIMESHIFT"}],"connections":[]}',
              }),
              // Result 6: CREATE_VIEW for "bearing_temp_rs"
              baseResult({
                statement_type: "CREATE_VIEW",
                entity_name: "bearing_temp_rs",
                source_streams: ["bearing_temp_bin"],
                field_map: { bearing_temp: 0 },
                program_json: '{"operators":[{"type":"TIMESHIFT"}],"connections":[]}',
              }),
              // Result 7: CREATE_VIEW for "input" (cross-join)
              baseResult({
                statement_type: "CREATE_VIEW",
                entity_name: "input",
                source_streams: ["vibration_rs", "bearing_temp_rs"],
                field_map: { vibration: 0, bearing_temp: 1 },
                program_json: '{"operators":[{"type":"CROSS_JOIN"}],"connections":[]}',
              }),
            ],
            new_ts_units_per_second: -1,
          };
          return JSON.stringify(expanded);
        }
        return wrapExpandedErrors([{ message: "unsupported", line: -1, column: -1 }]);
      },
      validateSql: () => JSON.stringify({ valid: true, errors: [] }),
    };

    const rtbotSql = new RtBotSql({ wasmModule: mockWasm });
    rtbotSql.execute(alignedSql);

    // --- Assert streams ---

    const vibStream = rtbotSql.getCatalog().lookupStream("vibration");
    expect(vibStream).toBeDefined();
    expect(vibStream!.columns).toHaveLength(1);
    expect(vibStream!.columns[0].name).toBe("value");

    const btStream = rtbotSql.getCatalog().lookupStream("bearing_temp");
    expect(btStream).toBeDefined();
    expect(btStream!.columns).toHaveLength(1);
    expect(btStream!.columns[0].name).toBe("value");

    // --- Assert views ---

    const vibBin = rtbotSql.getCatalog().lookupView("vibration_bin");
    expect(vibBin).toBeDefined();
    expect(vibBin!.entity_type).toBe("VIEW");
    expect(vibBin!.source_streams).toEqual(["vibration"]);

    const btBin = rtbotSql.getCatalog().lookupView("bearing_temp_bin");
    expect(btBin).toBeDefined();
    expect(btBin!.entity_type).toBe("VIEW");
    expect(btBin!.source_streams).toEqual(["bearing_temp"]);

    const vibRs = rtbotSql.getCatalog().lookupView("vibration_rs");
    expect(vibRs).toBeDefined();
    expect(vibRs!.source_streams).toEqual(["vibration_bin"]);

    const btRs = rtbotSql.getCatalog().lookupView("bearing_temp_rs");
    expect(btRs).toBeDefined();
    expect(btRs!.source_streams).toEqual(["bearing_temp_bin"]);

    const inputView = rtbotSql.getCatalog().lookupView("input");
    expect(inputView).toBeDefined();
    expect(inputView!.source_streams).toEqual(["vibration_rs", "bearing_temp_rs"]);
  });

  it("SET TIMESCALE updates internal timescale", () => {
    let capturedTsUnitsPerSecond: number | undefined;

    const mockWasm: RtBotSqlWasmModule = {
      compileSqlJson(sql: string, _catalogJson: string, tsUnitsPerSecond?: number): string {
        if (/SET\s+TIMESCALE/i.test(sql)) {
          // SET TIMESCALE ms → update to 1000 units/sec
          const expanded: ExpandedCompilationResult = {
            results: [],
            new_ts_units_per_second: 1000,
          };
          return JSON.stringify(expanded);
        }

        // Capture tsUnitsPerSecond on the second call (CREATE ALIGNED STREAM)
        capturedTsUnitsPerSecond = tsUnitsPerSecond;

        // Return a minimal aligned-stream expansion so catalog gets populated
        const expanded: ExpandedCompilationResult = {
          results: [
            baseResult({
              statement_type: "CREATE_STREAM",
              entity_name: "vibration",
              stream_schema: {
                name: "vibration",
                columns: [{ name: "value", index: 0 }],
              },
            }),
            baseResult({
              statement_type: "CREATE_STREAM",
              entity_name: "bearing_temp",
              stream_schema: {
                name: "bearing_temp",
                columns: [{ name: "value", index: 0 }],
              },
            }),
          ],
          new_ts_units_per_second: -1,
        };
        return JSON.stringify(expanded);
      },
      validateSql: () => JSON.stringify({ valid: true, errors: [] }),
    };

    const rtbotSql = new RtBotSql({ wasmModule: mockWasm });

    // First call: SET TIMESCALE — should update internal timescale to 1000
    rtbotSql.execute("SET TIMESCALE ms");

    // Second call: CREATE ALIGNED STREAM — mock captures tsUnitsPerSecond
    rtbotSql.execute("CREATE ALIGNED STREAM input (vibration DOUBLE, bearing_temp DOUBLE) BIN(1s)");

    // Verify the mock received the updated timescale
    expect(capturedTsUnitsPerSecond).toBe(1000);

    // Verify catalog was populated correctly
    expect(rtbotSql.getCatalog().lookupStream("vibration")).toBeDefined();
    expect(rtbotSql.getCatalog().lookupStream("bearing_temp")).toBeDefined();
  });
});

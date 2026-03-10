import {
  InMemoryCatalog,
  InMemoryStreamStore,
  RtBotSql,
  formatRow,
  StubPipelineRunner,
} from "../index";
import type {
  RtBotSqlWasmModule,
  CompilationResult,
  ValidationResult,
  CatalogSnapshot,
  Message,
} from "../index";

// --- Mock WASM module for unit tests ---

function createMockWasm(): RtBotSqlWasmModule {
  return {
    compileSqlJson(sql: string, catalogJson: string): string {
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
        return JSON.stringify(result);
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
        return JSON.stringify(result);
      }

      // Simple mock: parse SELECT
      const selectMatch = sql.match(/SELECT\s+(.+)\s+FROM\s+(\w+)/i);
      if (selectMatch) {
        const source = selectMatch[2];
        if (!catalog.streams[source] && !catalog.views[source]) {
          return JSON.stringify({
            errors: [
              { message: `unknown source: ${source}`, line: -1, column: -1 },
            ],
          });
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
        return JSON.stringify(result);
      }

      return JSON.stringify({
        errors: [
          { message: "unsupported statement type", line: -1, column: -1 },
        ],
      });
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

    const result = RtBotSql.compile("SELECT price FROM prices", catalog, wasm);
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

    const result = RtBotSql.compile(
      "SELECT x FROM nonexistent",
      catalog,
      wasm,
    );
    expect(result.errors.length).toBeGreaterThan(0);
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

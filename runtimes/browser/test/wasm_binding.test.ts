// Real-WASM tests for the browser binding's error-location serialization.
//
// Earlier tests in rtbot_sql.test.ts use a mock WasmModule, so they don't
// exercise the actual C++ bindings.cpp serializer. This file loads the
// real Emscripten output and verifies the JSON contract end-to-end.
//
// Loaded via `require` of the Emscripten loader script. Bazel data deps
// place the loader at a known location relative to this test file.

// Inline node ambient declarations so we don't need @types/node.
declare const __dirname: string;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
declare function require(id: string): any;

interface RealWasmModule {
  compileSqlJson(sql: string, catalogJson: string, tsUnitsPerSecond: number): string;
  validateSql(sql: string): string;
}

let wasm: RealWasmModule | null = null;

beforeAll(async () => {
  // The wasm_cc_binary output sits under runtimes/browser/rtbot-sql-wasm/
  // relative to the runfiles root. From this test file's compiled location
  // (runtimes/browser/test), the relative path is "../rtbot-sql-wasm".
  const loaderPath = __dirname + "/../rtbot-sql-wasm/bindings-cpp-wasm.js";
  const factory = require(loaderPath);
  wasm = await factory();
});

describe("Real WASM binding — error location fields", () => {
  it("validateSql returns end_line / end_column on syntax errors", () => {
    const json = wasm!.validateSql("SELEKT FROM x");
    const out = JSON.parse(json);
    expect(out.valid).toBe(false);
    expect(out.errors.length).toBeGreaterThan(0);
    const err = out.errors[0];
    expect(err).toHaveProperty("line");
    expect(err).toHaveProperty("column");
    expect(err).toHaveProperty("end_line");
    expect(err).toHaveProperty("end_column");
    expect(err.end_line).toBeGreaterThanOrEqual(1);
    expect(err.end_column).toBeGreaterThanOrEqual(1);
  });

  it("compileSqlJson returns end_line / end_column on semantic errors", () => {
    const catalogJson = JSON.stringify({
      streams: {},
      views: {},
      tables: {},
      dictionaries: {},
    });
    const json = wasm!.compileSqlJson(
      "SELECT * FROM nonexistent_stream LIMIT 10",
      catalogJson,
      1_000_000,
    );
    const out = JSON.parse(json);
    expect(out.results.length).toBeGreaterThan(0);
    const errors = out.results[0].errors;
    expect(errors.length).toBeGreaterThan(0);
    const err = errors[0];
    expect(err.end_line).toBeGreaterThanOrEqual(1);
    expect(err.end_column).toBeGreaterThanOrEqual(1);
  });

  it("compileSqlJson surfaces CREATE STREAM source metadata (default scalar)", () => {
    // No TYPE clause → effective type defaults to scalar; type_clause /
    // window_clause are omitted (no user-written tokens to span). The
    // analyzer accepts the omission and emits no diagnostics.
    const catalogJson = JSON.stringify({
      streams: {},
      views: {},
      tables: {},
      dictionaries: {},
    });
    const json = wasm!.compileSqlJson(
      'CREATE STREAM system_cpu (cpu DOUBLE, x TEXT) FROM "ignition://{x}/cpu";',
      catalogJson,
      1_000_000,
    );
    const out = JSON.parse(json);
    const result = out.results[0];
    expect(result.errors).toEqual([]);
    expect(result.statement_type).toBe("CREATE_STREAM");
    expect(result.stream_schema.name).toBe("system_cpu");
    expect(result.stream_schema.source).toBeDefined();
    const src = result.stream_schema.source;
    expect(src.name).toBe("ignition://{x}/cpu");
    expect(src.type).toBe("scalar");
    expect(src.type_clause).toBeUndefined();
    expect(src.window_clause).toBeUndefined();
    expect(src.line).toBeGreaterThanOrEqual(1);
    expect(src.end_column).toBeGreaterThan(src.column);
  });

  it("compileSqlJson surfaces csv_burst TYPE + WINDOW with token spans", () => {
    // Explicit TYPE csv_burst + WINDOW yields both clauses with their
    // own spans. The analyzer requires WINDOW alongside csv_burst, and
    // forbids it otherwise.
    const catalogJson = JSON.stringify({
      streams: {},
      views: {},
      tables: {},
      dictionaries: {},
    });
    const json = wasm!.compileSqlJson(
      'CREATE STREAM vibration (amplitude DOUBLE, x TEXT) FROM "ignition://{x}/burst" TYPE csv_burst WINDOW 20480;',
      catalogJson,
      1_000_000,
    );
    const out = JSON.parse(json);
    const result = out.results[0];
    expect(result.errors).toEqual([]);
    const src = result.stream_schema.source;
    expect(src.type).toBe("csv_burst");
    expect(src.type_clause).toBeDefined();
    expect(src.type_clause.value).toBe("csv_burst");
    expect(src.window_clause).toBeDefined();
    expect(src.window_clause.value).toBe(20480);
    expect(src.window_clause.end_column).toBeGreaterThan(src.window_clause.column);
  });

  it("compileSqlJson surfaces FROM placeholder validation errors", () => {
    // `{nope}` names no declared column → analyzer error with a span over
    // the placeholder, delivered through the WASM JSON boundary.
    const catalogJson = JSON.stringify({
      streams: {},
      views: {},
      tables: {},
      dictionaries: {},
    });
    const json = wasm!.compileSqlJson(
      "CREATE STREAM s(value DOUBLE) FROM 'ignition://{nope}/x';",
      catalogJson,
      1_000_000,
    );
    const out = JSON.parse(json);
    const result = out.results[0];
    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.errors[0].message).toContain(
      "FROM source references unknown column 'nope'",
    );
    expect(result.errors[0].end_column).toBeGreaterThan(
      result.errors[0].column,
    );
  });

  it("compileSqlJson surfaces materialized view output_target", () => {
    // The TO parent path is stored verbatim on the view result; absent
    // when the statement has no TO clause. One statement per call — the
    // host threads the catalog between statements.
    const catalogJson = JSON.stringify({
      streams: {
        trades: {
          name: "trades",
          columns: [
            { name: "price", index: 0, type: "DOUBLE" },
            { name: "instrument_id", index: 1, type: "TEXT" },
          ],
        },
      },
      views: {},
      tables: {},
      dictionaries: {},
    });
    const json = wasm!.compileSqlJson(
      "CREATE MATERIALIZED VIEW stats TO 'ignition://[Coprocessor]proc/stats/{instrument_id}/' " +
        "AS SELECT AVG(price) AS avg, instrument_id FROM trades GROUP BY instrument_id;",
      catalogJson,
      1_000_000,
    );
    const out = JSON.parse(json);
    const mv = out.results[0];
    expect(mv.errors).toEqual([]);
    expect(mv.output_target).toBe(
      "ignition://[Coprocessor]proc/stats/{instrument_id}/",
    );
    // Payload = numeric projected columns only; instrument_id (TEXT) is
    // path material.
    expect(mv.output_payload_columns).toEqual(["avg"]);
  });

  it("compileSqlJson returns end_line / end_column on AST converter errors (BETWEEN)", () => {
    const catalogJson = JSON.stringify({
      streams: {},
      views: {},
      tables: {},
      dictionaries: {},
    });
    const json = wasm!.compileSqlJson(
      "SELECT 1 FROM x WHERE x BETWEEN 1 AND 10 LIMIT 1",
      catalogJson,
      1_000_000,
    );
    const out = JSON.parse(json);
    const errors: Array<{ message: string; end_line: number; end_column: number }> =
      out.results[0].errors;
    const between = errors.find((e) => e.message.includes("BETWEEN"));
    expect(between).toBeDefined();
    expect(between!.end_line).toBeGreaterThanOrEqual(1);
    expect(between!.end_column).toBeGreaterThanOrEqual(1);
  });
});

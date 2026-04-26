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

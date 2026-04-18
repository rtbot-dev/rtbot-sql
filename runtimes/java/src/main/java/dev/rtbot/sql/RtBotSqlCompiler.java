package dev.rtbot.sql;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Low-level JNI bridge to the RTBot SQL C++ compiler and pipeline engine.
 *
 * <p>Every method maps 1:1 to a native function in the {@code rtbot_sql_jni}
 * shared library. All complex data crosses the boundary as JSON strings,
 * identical to the Python and WASM wrappers.
 *
 * <p><b>Thread safety:</b> The underlying engine is NOT thread-safe for the
 * same pipeline handle. Callers must ensure calls for the same handle are
 * never concurrent.
 */
public final class RtBotSqlCompiler {

    private static final String LIB_NAME = "rtbot_sql_jni";

    static {
        loadNativeLibrary();
    }

    private RtBotSqlCompiler() {}

    // -----------------------------------------------------------------
    // SQL preprocessing
    // -----------------------------------------------------------------

    /**
     * Preprocess a SQL statement, expanding sugar syntax like
     * {@code CREATE ALIGNED STREAM ... BIN()} and {@code SET TIMESCALE}.
     *
     * @param sql                the SQL statement to preprocess
     * @param tsUnitsPerSecond   current timescale (e.g. 1000000 for microseconds)
     * @return JSON string with {@code "statements"} array and
     *         {@code "new_ts_units_per_second"} (-1 if unchanged)
     */
    public static native String preprocessSqlJson(String sql, long tsUnitsPerSecond);

    // -----------------------------------------------------------------
    // SQL compilation
    // -----------------------------------------------------------------

    /**
     * Preprocess and compile a SQL statement against a catalog snapshot.
     *
     * <p>Sugar syntax (CREATE ALIGNED STREAM ... BIN()) is expanded and each
     * resulting statement is compiled against an incrementally updated catalog.
     * SET TIMESCALE returns empty results with the new timescale value.
     *
     * @param sql              the SQL statement to compile
     * @param catalogJson      JSON representation of the catalog snapshot
     * @param tsUnitsPerSecond current timescale (e.g. 1000000 for microseconds)
     * @return JSON string with {@code "results"} array of compilation results
     *         and {@code "new_ts_units_per_second"} (-1 if unchanged)
     */
    public static native String compileSqlJson(String sql, String catalogJson, long tsUnitsPerSecond);

    /**
     * Validate a SQL statement (syntax check only, no catalog required).
     *
     * @param sql the SQL statement to validate
     * @return JSON string with validation result
     */
    public static native String validateSql(String sql);

    /**
     * Consolidate every view currently registered in the catalog into one
     * rtbot Program. Each view's body is inlined; Output operators at view
     * boundaries are dropped (the Collector-sink mechanism in Program.h
     * replaces them). Exactly one Input operator is retained per base
     * stream as the session's entry. Materialized views are exposed as
     * named outputs.
     *
     * <p>Returns JSON with:
     * <ul>
     *   <li>{@code program_json} — consolidated Program JSON.</li>
     *   <li>{@code view_terminals} — view_name → operator id producing
     *       that view's output in the consolidated graph.</li>
     *   <li>{@code view_terminal_ports} — view_name → output port id on
     *       that terminal operator.</li>
     *   <li>{@code materialized_views} — subset of view_terminals keys
     *       exposed as Program outputs.</li>
     *   <li>{@code base_stream_inputs} — base_stream_name → Input
     *       operator id in the consolidated graph.</li>
     *   <li>{@code base_stream_ports} — base_stream_name → port id.</li>
     *   <li>{@code errors} — non-empty if compilation fails.</li>
     * </ul>
     *
     * @param catalogJson JSON representation of the catalog snapshot (same
     *                    shape as the catalog passed to {@link #compileSqlJson})
     */
    public static native String compileSessionJson(String catalogJson);

    // -----------------------------------------------------------------
    // Pipeline lifecycle
    // -----------------------------------------------------------------

    /**
     * Create a native pipeline from a program JSON definition.
     *
     * @param programJson RTBot program JSON (from compilation)
     * @return opaque native handle for the pipeline
     */
    public static native long createPipeline(String programJson);

    /**
     * Feed a single timestamped message into a pipeline.
     *
     * @param handle    native pipeline handle from {@link #createPipeline}
     * @param timestamp monotonically increasing timestamp
     * @param values    value vector for this message
     * @param port      input port identifier (e.g. "i1", "i2")
     * @return JSON string of output messages array
     */
    public static native String feedPipeline(long handle, long timestamp, double[] values, String port);

    /**
     * Feed a single 3-value message into a pipeline (hot-path overload).
     *
     * <p>Avoids array marshalling overhead on the JVM/JNI boundary for the
     * common 3-column ingest case (device_id, channel_id, amplitude).
     */
    public static native String feedPipeline3(long handle, long timestamp,
                                              double v0, double v1, double v2,
                                              String port);

    /**
     * Hot-path overload for 3-value messages on default input port "i1".
     */
    public static native String feedPipeline3I1(long handle, long timestamp,
                                                double v0, double v1, double v2);

    /**
     * Feed a batch of messages on default input port "i1".
     *
     * <p>{@code columns} is column-major: {@code columns[c]} is the array of
     * values for column {@code c}, and all arrays must have the same length as
     * {@code timestamps}. The native side feeds the full batch in one call via
     * Program::receive_batch, then returns all produced output messages as JSON.
     */
    public static native String feedPipelineBatchI1(
            long handle,
            long[] timestamps,
            double[][] columns);

    /**
     * Raw-buffer variant of the batched feed. Same Java-side shape as
     * {@link #feedPipelineBatchI1} (column-major {@code double[][]} plus a
     * parallel {@code long[]} timestamps array), but the native side skips
     * per-row {@code Message<VectorNumberData>} allocation and streams the
     * row span straight into the entry operator via
     * {@code Program::receive_buffer}. Operators that override
     * {@code receive_data_buffer} (e.g. BurstAggregate) benefit the most;
     * other operators fall back to the default per-row Message creation.
     */
    public static native String feedPipelineBufferI1(
            long handle,
            long[] timestamps,
            double[][] columns);

    /**
     * Reset cumulative native feed-path profiling counters.
     */
    public static native void resetNativeFeedStats();

    /**
     * Enable or disable native feed-path profiling counters.
     */
    public static native void setNativeFeedStatsEnabled(boolean enabled);

    /**
     * Return native feed-path profiling counters as JSON object.
     */
    public static native String getNativeFeedStatsJson();

    /**
     * Destroy a native pipeline and free all associated resources.
     *
     * @param handle native pipeline handle
     */
    public static native void destroyPipeline(long handle);

    /**
     * Serialize the full pipeline state to JSON.
     *
     * @param handle native pipeline handle
     * @return JSON string of serialized pipeline state
     */
    public static native String serializePipeline(long handle);

    /**
     * Restore a pipeline from a previously serialized JSON state.
     *
     * @param handle    native pipeline handle
     * @param stateJson JSON state from {@link #serializePipeline}
     */
    public static native void restorePipeline(long handle, String stateJson);

    // -----------------------------------------------------------------
    // Library loading
    // -----------------------------------------------------------------

    /**
     * Loads the platform-specific native library.
     *
     * <p>Strategy:
     * <ol>
     *   <li>Look for the library as a classpath resource under
     *       {@code /native/<platform>/}. If found, extract to a temp file
     *       and load via {@link System#load(String)}.
     *   <li>Fall back to {@link System#loadLibrary(String)} when the
     *       resource is absent. This covers Bazel test environments where
     *       the library is placed on {@code java.library.path} via the
     *       {@code data} attribute.
     * </ol>
     */
    private static void loadNativeLibrary() {
        String resourcePath = nativeResourcePath();
        try (InputStream in = RtBotSqlCompiler.class.getResourceAsStream(resourcePath)) {
            if (in != null) {
                Path tmp = Files.createTempFile("rtbot_sql_jni_", platformSuffix());
                tmp.toFile().deleteOnExit();
                Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
                System.load(tmp.toAbsolutePath().toString());
            } else {
                System.loadLibrary(LIB_NAME);
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to load RTBot SQL native library from " + resourcePath, e);
        }
    }

    private static String nativeResourcePath() {
        String os = System.getProperty("os.name", "").toLowerCase();
        String arch = System.getProperty("os.arch", "").toLowerCase();
        String platform;
        if (os.contains("linux")) {
            platform = "linux-" + (arch.contains("aarch64") ? "aarch64" : "x86_64");
        } else if (os.contains("mac")) {
            platform = "mac-" + (arch.contains("aarch64") ? "aarch64" : "x86_64");
        } else if (os.contains("win")) {
            platform = "win-" + (arch.contains("aarch64") ? "aarch64" : "x86_64");
        } else {
            platform = "unknown";
        }
        return "/native/" + platform + "/" + System.mapLibraryName(LIB_NAME);
    }

    private static String platformSuffix() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("win")) return ".dll";
        if (os.contains("mac")) return ".dylib";
        return ".so";
    }
}

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
            platform = "win-x86_64";
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

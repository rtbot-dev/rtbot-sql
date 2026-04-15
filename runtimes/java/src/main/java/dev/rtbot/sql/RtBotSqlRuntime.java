package dev.rtbot.sql;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Main orchestrator for the RTBot SQL Java runtime.
 *
 * <p>Port of Python's {@code RtBotSql}. Maintains an in-memory catalog, stream
 * store, and pipeline runner, executing SQL statements through the native C++
 * compiler and RTBot engine.
 *
 * <p>Supports:
 * <ul>
 *   <li>CREATE STREAM / CREATE TABLE</li>
 *   <li>INSERT INTO</li>
 *   <li>CREATE VIEW / CREATE MATERIALIZED VIEW</li>
 *   <li>DROP</li>
 *   <li>SELECT (tier-1 read and tier-2/3 ephemeral pipeline)</li>
 * </ul>
 *
 * <p><b>Thread safety:</b> Not thread-safe. Callers must synchronize externally.
 */
public class RtBotSqlRuntime {

    private final InMemoryCatalog catalog = new InMemoryCatalog();
    private final InMemoryStreamStore store = new InMemoryStreamStore();
    private final LocalPipelineRunner runner = new LocalPipelineRunner();

    /** view name -> pipeline ID */
    private final Map<String, String> viewPipelines = new HashMap<>();
    /** view name -> (source stream name -> port) */
    private final Map<String, Map<String, String>> viewPortMaps = new HashMap<>();
    /** source stream name -> set of dependent view names */
    private final Map<String, Set<String>> dependencies = new HashMap<>();

    /** stream/view name -> active output listener (subscription registry) */
    private final Map<String, OutputListener> subscriptions = new ConcurrentHashMap<>();

    /**
     * When true, all messages (raw streams and materialized views) are stored
     * in the in-memory store — suitable for notebooks, testing, and scenarios
     * where SELECT queries need access to historical data.
     *
     * <p>When false (default), data is only forwarded to active subscribers
     * and discarded otherwise. This is the production-grade subscription model:
     * no internal lists grow over time, providing stable memory behavior for
     * long-running processes.
     *
     * <p>Plain VIEWs (non-materialized) never store output regardless of this
     * setting — they only propagate to dependents and subscribers.
     */
    private boolean collectMode = false;

    private long lastTimestamp = System.currentTimeMillis();
    private final Gson gson = new Gson();
    private long tsUnitsPerSecond = 1_000_000L;

    private static final Pattern LIMIT_PATTERN =
            Pattern.compile("\\bLIMIT\\s+(\\d+)\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern CREATE_STREAM_PATTERN =
            Pattern.compile("^\\s*CREATE\\s+STREAM\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern DROP_STREAM_PATTERN =
            Pattern.compile("^\\s*DROP\\s+STREAM\\b", Pattern.CASE_INSENSITIVE);

    // =================================================================
    // Public API
    // =================================================================

    /**
     * Execute a SQL statement and return the result.
     *
     * @param sql the SQL statement to execute
     * @return a {@link SelectResult} for SELECT statements, or {@code null} for DDL/DML
     * @throws SqlError if compilation or execution fails
     */
    public Object execute(String sql) {
        String catalogJson = catalog.snapshotJson();
        String resultJson = RtBotSqlCompiler.compileSqlJson(sql, catalogJson, tsUnitsPerSecond);

        com.google.gson.JsonObject wrapper = gson.fromJson(resultJson, com.google.gson.JsonObject.class);

        // Handle SET TIMESCALE
        long newTsUnitsPerSecond = wrapper.get("new_ts_units_per_second").getAsLong();
        if (newTsUnitsPerSecond > 0) {
            tsUnitsPerSecond = newTsUnitsPerSecond;
            return null;
        }

        com.google.gson.JsonArray results = wrapper.getAsJsonArray("results");
        Object lastSelectResult = null;

        for (int i = 0; i < results.size(); i++) {
            CompilationResult result = gson.fromJson(results.get(i), CompilationResult.class);

            if (result.hasErrors()) {
                throw new SqlError(result.errors);
            }

            StatementType stType = result.statementTypeEnum();
            if (stType == null) {
                throw new SqlError("Unknown statement type: " + result.statementType);
            }

            switch (stType) {
                case CREATE_STREAM:
                    handleCreateStream(result);
                    break;
                case INSERT:
                    handleInsert(result);
                    break;
                case CREATE_VIEW:
                case CREATE_MATERIALIZED_VIEW:
                    handleCreateView(result);
                    break;
                case DROP:
                    handleDrop(result);
                    break;
                case SELECT:
                    lastSelectResult = handleSelect(sql, result);
                    break;
                default:
                    throw new SqlError("Unsupported statement type: " + result.statementType);
            }
        }

        return lastSelectResult;
    }

    /**
     * Insert a row with mixed types (doubles and strings for TEXT columns).
     *
     * <p>Strings are dictionary-encoded to doubles before passing to the
     * existing {@link #insert(String, long, List)} pipeline. Each TEXT column
     * gets its own {@link StringDictionary} keyed by
     * {@code streamName + "." + columnName}.
     *
     * @param streamName the target stream
     * @param timestamp  the explicit timestamp (must be monotonically increasing per stream)
     * @param values     mixed list — {@link String} for TEXT columns, {@link Number} for DOUBLE columns
     * @throws IllegalArgumentException if streamName is null, stream doesn't exist,
     *         column count mismatches, or value types don't match column types
     */
    public void insertMixed(String streamName, long timestamp, List<Object> values) {
        if (streamName == null) {
            throw new IllegalArgumentException("streamName must not be null");
        }
        StreamSchema schema = catalog.lookupStream(streamName);
        if (schema == null) {
            throw new IllegalArgumentException("unknown stream: " + streamName);
        }
        if (values.size() != schema.columns.size()) {
            throw new IllegalArgumentException(
                "expected " + schema.columns.size() + " values, got " + values.size());
        }

        List<Double> encoded = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            ColumnDef col = schema.columns.get(i);
            Object val = values.get(i);

            if (col.isText()) {
                if (!(val instanceof String)) {
                    throw new IllegalArgumentException(
                        "expected String for TEXT column '" + col.name
                        + "', got " + val.getClass().getSimpleName());
                }
                String dictKey = streamName + "." + col.name;
                StringDictionary dict = catalog.getOrCreateDictionary(dictKey);
                encoded.add(dict.encode((String) val));
            } else {
                if (!(val instanceof Number)) {
                    throw new IllegalArgumentException(
                        "expected Number for DOUBLE column '" + col.name
                        + "', got " + val.getClass().getSimpleName());
                }
                encoded.add(((Number) val).doubleValue());
            }
        }

        insert(streamName, timestamp, encoded);
    }

    /**
     * Insert a message with an explicit timestamp into a stream and propagate
     * to dependent views.
     *
     * <p>This is the Java equivalent of Python's {@code insert_dataframe} —
     * enables precise timestamp control for testing and replay scenarios.
     *
     * @param streamName the target stream
     * @param timestamp  the explicit timestamp (must be monotonically increasing per stream)
     * @param values     the value vector (must match stream schema column count)
     * @throws SqlError if the stream does not exist or payload length mismatches
     */
    public void insert(String streamName, long timestamp, List<Double> values) {
        StreamSchema schema = catalog.lookupStream(streamName);
        if (schema == null) {
            throw new SqlError("Unknown stream: " + streamName);
        }

        List<Double> payload = new ArrayList<>(values);

        if (schema.columns != null && !schema.columns.isEmpty()
                && payload.size() != schema.columns.size()) {
            throw new SqlError("INSERT payload length mismatch for " + streamName
                    + ": expected " + schema.columns.size() + ", got " + payload.size());
        }

        appendAndPropagate(streamName, timestamp, payload);
    }

    /**
     * Optimized insert path for common 3-column numeric streams.
     *
     * <p>Skips per-message list/boxing allocations when collect mode is off
     * and there is no direct subscriber on the source stream.
     */
    public void insert3(String streamName, long timestamp,
                        double v0, double v1, double v2) {
        StreamSchema schema = catalog.lookupStream(streamName);
        if (schema == null) {
            throw new SqlError("Unknown stream: " + streamName);
        }
        if (schema.columns != null && !schema.columns.isEmpty()
                && schema.columns.size() != 3) {
            throw new SqlError("insert3 requires 3 columns for " + streamName
                    + ", found " + schema.columns.size());
        }

        // Fallback when source stream needs direct materialization/callback.
        if (collectMode || subscriptions.containsKey(streamName)) {
            insert(streamName, timestamp, java.util.Arrays.asList(v0, v1, v2));
            return;
        }

        Set<String> dependents = dependencies.get(streamName);
        if (dependents == null) {
            return;
        }

        for (String dependent : dependents) {
            String pipelineId = viewPipelines.get(dependent);
            if (pipelineId == null) continue;
            Map<String, String> portMap =
                    viewPortMaps.getOrDefault(dependent, Collections.emptyMap());
            String port = portMap.getOrDefault(streamName, "i1");
            List<OutputMessage> outputs = runner.feed3(
                    pipelineId, timestamp, v0, v1, v2, port);
            for (OutputMessage out : outputs) {
                appendAndPropagate(dependent, out.timestamp, out.values);
            }
        }
    }

    /**
     * Batched insert path for common 3-column numeric streams.
     *
     * <p>Feeds a whole batch of rows in one native call for single-source
     * dependents on port {@code i1}. Falls back to row-by-row routing when
     * collect mode is enabled, the source has direct subscribers, or a
     * dependent requires a non-default input port.
     */
    public void insert3Batch(String streamName,
                             long[] timestamps,
                             double[] v0,
                             double[] v1,
                             double[] v2) {
        StreamSchema schema = catalog.lookupStream(streamName);
        if (schema == null) {
            throw new SqlError("Unknown stream: " + streamName);
        }
        if (schema.columns != null && !schema.columns.isEmpty()
                && schema.columns.size() != 3) {
            throw new SqlError("insert3Batch requires 3 columns for " + streamName
                    + ", found " + schema.columns.size());
        }
        if (timestamps == null || v0 == null || v1 == null || v2 == null) {
            throw new SqlError("insert3Batch arrays must not be null");
        }
        int n = timestamps.length;
        if (v0.length != n || v1.length != n || v2.length != n) {
            throw new SqlError("insert3Batch array length mismatch for " + streamName
                    + ": ts=" + n
                    + ", v0=" + v0.length
                    + ", v1=" + v1.length
                    + ", v2=" + v2.length);
        }
        if (n == 0) {
            return;
        }

        // Fallback when source stream needs direct materialization/callback.
        if (collectMode || subscriptions.containsKey(streamName)) {
            for (int i = 0; i < n; i++) {
                insert(streamName, timestamps[i], java.util.Arrays.asList(v0[i], v1[i], v2[i]));
            }
            return;
        }

        Set<String> dependents = dependencies.get(streamName);
        if (dependents == null) {
            return;
        }

        for (String dependent : dependents) {
            String pipelineId = viewPipelines.get(dependent);
            if (pipelineId == null) continue;
            Map<String, String> portMap =
                    viewPortMaps.getOrDefault(dependent, Collections.emptyMap());
            String port = portMap.getOrDefault(streamName, "i1");

            if ("i1".equals(port)) {
                List<OutputMessage> outputs = runner.feedBatch3I1(
                        pipelineId, timestamps, v0, v1, v2);
                for (OutputMessage out : outputs) {
                    appendAndPropagate(dependent, out.timestamp, out.values);
                }
            } else {
                // Multi-source/non-default-port case: preserve current semantics.
                for (int i = 0; i < n; i++) {
                    List<OutputMessage> outputs = runner.feed3(
                            pipelineId, timestamps[i], v0[i], v1[i], v2[i], port);
                    for (OutputMessage out : outputs) {
                        appendAndPropagate(dependent, out.timestamp, out.values);
                    }
                }
            }
        }
    }

    // =================================================================
    // Subscription API
    // =================================================================

    /**
     * Subscribe to output messages from a stream or view.
     *
     * <p>When a listener is registered, every message produced for the given
     * stream/view is forwarded to the listener. In streaming mode, this is the
     * only way to observe output — data is not accumulated in memory.
     *
     * <p>Follows the rtbot-redis pattern: output is only delivered when there
     * is an active subscriber. If no subscriber exists and streaming mode is
     * enabled, output is discarded to maintain memory stability.
     *
     * @param streamOrViewName the stream or view to subscribe to
     * @param listener         the callback to receive output messages
     * @throws IllegalArgumentException if streamOrViewName or listener is null
     */
    public void subscribe(String streamOrViewName, OutputListener listener) {
        if (streamOrViewName == null || listener == null) {
            throw new IllegalArgumentException("streamOrViewName and listener must not be null");
        }
        subscriptions.put(streamOrViewName, listener);
    }

    /**
     * Remove the subscription for a stream or view.
     *
     * <p>After unsubscribing, no further messages are forwarded to the
     * previously registered listener. In streaming mode, subsequent output
     * for this stream/view will be discarded.
     *
     * @param streamOrViewName the stream or view to unsubscribe from
     */
    public void unsubscribe(String streamOrViewName) {
        if (streamOrViewName != null) {
            subscriptions.remove(streamOrViewName);
        }
    }

    /**
     * Check whether a stream or view has an active subscriber.
     *
     * @param streamOrViewName the stream or view name
     * @return true if there is an active listener
     */
    public boolean hasSubscriber(String streamOrViewName) {
        return subscriptions.containsKey(streamOrViewName);
    }

    /**
     * Enable or disable collect mode.
     *
     * <p>In collect mode:
     * <ul>
     *   <li>All messages (raw streams and materialized views) are stored in
     *       the in-memory store</li>
     *   <li>SELECT queries work on accumulated historical data</li>
     *   <li>Suitable for notebooks, testing, and interactive exploration</li>
     * </ul>
     *
     * <p>When collect mode is disabled (default), the runtime uses a
     * subscription model: output is forwarded only to active subscribers
     * and discarded otherwise. No internal lists grow over time, providing
     * production-grade memory stability.
     *
     * <p>Plain VIEWs (non-materialized) never store output regardless of
     * this setting.
     *
     * @param enabled true to enable collect mode (store all data)
     */
    public void setCollectMode(boolean enabled) {
        this.collectMode = enabled;
    }

    /**
     * Returns whether collect mode is enabled.
     *
     * @return true if collect mode is active (all data stored in memory)
     */
    public boolean isCollectMode() {
        return collectMode;
    }

    /**
     * Reset feed-path performance counters in both Java and native layers.
     */
    public void resetPerfStats() {
        runner.setPerfStatsEnabled(true);
        RtBotSqlCompiler.setNativeFeedStatsEnabled(true);
        runner.resetPerfStats();
        RtBotSqlCompiler.resetNativeFeedStats();
    }

    /**
     * Snapshot feed-path performance counters for Java + native execution.
     *
     * @return map of cumulative counters in nanoseconds / counts
     */
    public Map<String, Long> getPerfStats() {
        Map<String, Long> stats = new LinkedHashMap<>();
        stats.putAll(runner.snapshotPerfStats());
        stats.putAll(parseLongCountersJson(RtBotSqlCompiler.getNativeFeedStatsJson()));
        return stats;
    }

    // =================================================================
    // Output decoding
    // =================================================================

    /**
     * Decode a raw output row, converting TEXT column dictionary IDs back to strings.
     *
     * <p>For each column in the stream schema, if the column is TEXT, the
     * corresponding double value is looked up in the stream's
     * {@link StringDictionary} (keyed by {@code "streamName.columnName"}).
     * If the lookup succeeds, the string replaces the numeric ID in the
     * returned list; otherwise the raw double is preserved.
     *
     * <p>If the stream is unknown or values is null, the raw doubles are
     * returned as-is.
     *
     * @param streamName the stream/view name for schema lookup
     * @param values     raw double values from the pipeline
     * @return decoded values — String for TEXT columns, Double for DOUBLE columns
     */
    public List<Object> decodeRow(String streamName, List<Double> values) {
        if (values == null) {
            return new ArrayList<>();
        }

        StreamSchema schema = catalog.lookupStream(streamName);
        if (schema != null) {
            return decodeRowFromSchema(streamName, schema, values);
        }

        // Fall back to view: use the view's fieldMap + source stream schemas
        // to determine which output columns are TEXT and need decoding.
        ViewMeta view = catalog.lookupView(streamName);
        if (view != null) {
            return decodeRowFromView(view, values);
        }

        return new ArrayList<>(values);
    }

    /**
     * Decode using a stream schema directly — the stream name is used as the
     * dictionary key prefix.
     */
    private List<Object> decodeRowFromSchema(String streamName, StreamSchema schema, List<Double> values) {
        List<Object> decoded = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            if (i < schema.columns.size() && schema.columns.get(i).isText()) {
                String dictKey = streamName + "." + schema.columns.get(i).name;
                StringDictionary dict = catalog.lookupDictionary(dictKey);
                if (dict != null) {
                    String str = dict.decode(values.get(i));
                    decoded.add(str != null ? str : values.get(i));
                } else {
                    decoded.add(values.get(i));
                }
            } else {
                decoded.add(values.get(i));
            }
        }
        return decoded;
    }

    /**
     * Decode a view output row by matching view field names to source stream
     * column definitions. TEXT columns are dictionary-encoded as doubles during
     * insertion into the source stream; the dictionary keys use the source
     * stream name prefix ({@code "sourceStream.columnName"}).
     *
     * <p>For each view output column, if a matching TEXT column exists in any
     * source stream, the corresponding dictionary is used to decode the value.
     */
    private List<Object> decodeRowFromView(ViewMeta view, List<Double> values) {
        if (view.fieldMap == null || view.fieldMap.isEmpty()) {
            return new ArrayList<>(values);
        }

        // Build ordered column list from fieldMap (sorted by index)
        List<Map.Entry<String, Integer>> sortedFields = new ArrayList<>(view.fieldMap.entrySet());
        sortedFields.sort(Map.Entry.comparingByValue());

        List<Object> decoded = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            if (i < sortedFields.size()) {
                String columnName = sortedFields.get(i).getKey();
                Object decodedValue = tryDecodeViewColumn(view, columnName, values.get(i));
                decoded.add(decodedValue);
            } else {
                decoded.add(values.get(i));
            }
        }
        return decoded;
    }

    /**
     * Tries to decode a single view output column value by searching the
     * view's source streams for a matching TEXT column and its dictionary.
     */
    private Object tryDecodeViewColumn(ViewMeta view, String columnName, Double value) {
        if (view.sourceStreams == null) {
            return value;
        }
        for (String source : view.sourceStreams) {
            StreamSchema sourceSchema = catalog.lookupStream(source);
            if (sourceSchema == null) {
                continue;
            }
            for (ColumnDef col : sourceSchema.columns) {
                if (col.name.equals(columnName) && col.isText()) {
                    String dictKey = source + "." + columnName;
                    StringDictionary dict = catalog.lookupDictionary(dictKey);
                    if (dict != null) {
                        String str = dict.decode(value);
                        return str != null ? str : value;
                    }
                }
            }
        }
        return value;
    }

    // =================================================================
    // Testing accessors
    // =================================================================

    /**
     * Returns the stream store (for testing).
     */
    InMemoryStreamStore getStore() {
        return store;
    }

    /**
     * Returns the catalog (for testing).
     */
    InMemoryCatalog getCatalog() {
        return catalog;
    }

    // =================================================================
    // SQL normalization
    // =================================================================

    /**
     * Normalize SQL by replacing RTBot-specific syntax with standard SQL.
     * "CREATE STREAM" -> "CREATE TABLE", "DROP STREAM" -> "DROP TABLE".
     */
    private static String normalizeSql(String sql) {
        String out = sql.strip();
        out = CREATE_STREAM_PATTERN.matcher(out).replaceFirst("CREATE TABLE");
        out = DROP_STREAM_PATTERN.matcher(out).replaceFirst("DROP TABLE");
        return out;
    }

    // =================================================================
    // Statement handlers
    // =================================================================

    /**
     * Handle CREATE STREAM: register the stream schema in the catalog.
     */
    private void handleCreateStream(CompilationResult result) {
        catalog.registerStream(result.entityName, result.streamSchema);
    }

    /**
     * Handle INSERT: validate payload and propagate to the stream.
     */
    private void handleInsert(CompilationResult result) {
        String streamName = result.entityName;
        StreamSchema schema = catalog.lookupStream(streamName);
        if (schema == null) {
            throw new SqlError("Unknown stream: " + streamName);
        }

        List<Double> payload = new ArrayList<>();
        if (result.insertPayload != null) {
            for (Double v : result.insertPayload) {
                payload.add(v != null ? v : 0.0);
            }
        }

        if (schema.columns != null && !schema.columns.isEmpty()
                && payload.size() != schema.columns.size()) {
            throw new SqlError("INSERT payload length mismatch for " + streamName
                    + ": expected " + schema.columns.size() + ", got " + payload.size());
        }

        // Sync dictionary updates from C++ compiler back to Java catalog.
        if (result.dictionaryUpdates != null) {
            for (Map.Entry<String, Map<String, String>> entry : result.dictionaryUpdates.entrySet()) {
                String dictKey = entry.getKey();
                StringDictionary dict = catalog.getOrCreateDictionary(dictKey);
                for (Map.Entry<String, String> mapping : entry.getValue().entrySet()) {
                    double id = Double.parseDouble(mapping.getKey());
                    String str = mapping.getValue();
                    dict.putMapping(id, str);
                }
            }
        }

        appendAndPropagate(streamName, nextTimestamp(), payload);
    }

    /**
     * Handle CREATE VIEW / CREATE MATERIALIZED VIEW:
     * register metadata, deploy pipeline, set up dependencies, backfill.
     */
    private void handleCreateView(CompilationResult result) {
        String name = result.entityName;
        StatementType stType = result.statementTypeEnum();
        boolean materialized = (stType == StatementType.CREATE_MATERIALIZED_VIEW);

        ViewMeta viewMeta = new ViewMeta();
        viewMeta.name = name;
        viewMeta.entityType = materialized
                ? EntityType.MATERIALIZED_VIEW.name()
                : EntityType.VIEW.name();
        viewMeta.viewType = result.viewType;
        viewMeta.fieldMap = result.fieldMap != null ? new HashMap<>(result.fieldMap) : new HashMap<>();
        viewMeta.sourceStreams = result.sourceStreams != null ? new ArrayList<>(result.sourceStreams) : new ArrayList<>();
        viewMeta.programJson = result.programJson;
        viewMeta.outputStream = name;
        viewMeta.perKeyPrefix = name + ":key:";
        viewMeta.knownKeys = new ArrayList<>();
        viewMeta.keyIndex = result.keyIndex;

        catalog.registerView(name, viewMeta);

        // Set up dependencies: source -> dependent view
        for (String source : viewMeta.sourceStreams) {
            dependencies.computeIfAbsent(source, k -> new HashSet<>()).add(name);
        }

        // Deploy the pipeline
        Map<String, String> outputConfig = new HashMap<>();
        outputConfig.put("output_stream", name);
        String pipelineId = runner.deploy(result.programJson, viewMeta.sourceStreams, outputConfig);
        viewPipelines.put(name, pipelineId);

        // Build port map: source stream name -> input port
        Map<String, String> sourcePortMap = new HashMap<>();
        for (int i = 0; i < viewMeta.sourceStreams.size(); i++) {
            sourcePortMap.put(viewMeta.sourceStreams.get(i), "i" + (i + 1));
        }
        viewPortMaps.put(name, sourcePortMap);

        // Backfill the new view from already-known source data.
        // Uses interleaved replay (all sources sorted by global timestamp)
        // to preserve temporal ordering that RTBot operators expect.
        backfillInterleaved(name, viewMeta, sourcePortMap, pipelineId);
    }

    /**
     * Backfill a view by replaying all source messages in global timestamp order.
     *
     * <p>Interleaved replay preserves the temporal ordering that RTBot operators
     * expect. Each message is fed to the program on its assigned input port —
     * the program's own time synchronization determines when output is produced.
     */
    private void backfillInterleaved(String name, ViewMeta viewMeta,
                                     Map<String, String> sourcePortMap, String pipelineId) {
        // Collect all events from all sources with their port assignments
        List<long[]> eventKeys = new ArrayList<>(); // [timestamp, sourceIndex]
        List<String> eventPorts = new ArrayList<>();
        List<List<Double>> eventValues = new ArrayList<>();

        for (int i = 0; i < viewMeta.sourceStreams.size(); i++) {
            String source = viewMeta.sourceStreams.get(i);
            String port = sourcePortMap.getOrDefault(source, "i" + (i + 1));
            for (Message msg : store.read(source)) {
                eventKeys.add(new long[]{msg.timestamp, i});
                eventPorts.add(port);
                eventValues.add(new ArrayList<>(msg.values));
            }
        }

        // Sort by (timestamp, sourceIndex) for deterministic ordering
        Integer[] indices = new Integer[eventKeys.size()];
        for (int i = 0; i < indices.length; i++) indices[i] = i;
        java.util.Arrays.sort(indices, (a, b) -> {
            long[] ea = eventKeys.get(a);
            long[] eb = eventKeys.get(b);
            if (ea[0] != eb[0]) return Long.compare(ea[0], eb[0]);
            return Long.compare(ea[1], eb[1]);
        });

        for (int idx : indices) {
            List<OutputMessage> outputs = runner.feed(
                    pipelineId, eventKeys.get(idx)[0], eventValues.get(idx), eventPorts.get(idx));
            for (OutputMessage out : outputs) {
                appendAndPropagate(name, out.timestamp, out.values);
            }
        }
    }

    /**
     * Handle DROP: destroy pipeline, remove from catalog, clear store, clean up dependencies.
     */
    private void handleDrop(CompilationResult result) {
        String name = result.dropEntityName;

        // Destroy the pipeline if it exists
        if (viewPipelines.containsKey(name)) {
            runner.destroy(viewPipelines.get(name));
            viewPipelines.remove(name);
        }
        viewPortMaps.remove(name);

        // Remove from catalog and store
        catalog.drop(name);
        store.clear(name);

        // Clean up dependencies
        for (Set<String> dependents : dependencies.values()) {
            dependents.remove(name);
        }
        dependencies.remove(name);
    }

    /**
     * Handle SELECT: dispatch to tier-1 read or ephemeral pipeline based on select tier.
     */
    private SelectResult handleSelect(String sql, CompilationResult result) {
        SelectTier tier = result.selectTierEnum();
        if (tier == SelectTier.TIER1_READ) {
            return executeTier1(sql, result);
        }
        return executeWithPipeline(sql, result);
    }

    // =================================================================
    // Propagation engine
    // =================================================================

    /**
     * Append a message to a stream/view, notify subscribers, and propagate
     * to all dependents.
     *
     * <p>Storage behavior depends on collect mode:
     * <ul>
     *   <li><b>Collect mode ON:</b> All messages are stored in the in-memory
     *       store. Suitable for notebooks and testing where SELECT queries
     *       need access to all historical data.</li>
     *   <li><b>Collect mode OFF (default):</b> Messages are forwarded to
     *       active subscribers and discarded. No internal lists grow over
     *       time — production-grade memory stability.</li>
     * </ul>
     *
     * <p>When a subscriber is registered for a stream/view, the message is
     * always forwarded to the listener — regardless of collect mode.
     *
     * <p>Plain VIEWs (non-materialized) never store output — they only
     * propagate to dependents and subscribers.
     */
    private void appendAndPropagate(String streamName, long timestamp, List<Double> values) {
        ViewMeta view = catalog.lookupView(streamName);
        boolean isPlainView = (view != null
                && EntityType.VIEW.name().equals(view.entityType));

        if (!isPlainView && collectMode) {
            store.append(streamName, timestamp, values);

            // Track keys for keyed views
            if (view != null
                    && ViewType.KEYED.name().equals(view.viewType)
                    && view.keyIndex >= 0
                    && view.keyIndex < values.size()) {
                catalog.addKey(streamName, values.get(view.keyIndex));
            }
        }

        // Notify subscriber if one is registered for this stream/view.
        // This follows the rtbot-redis pattern: output is forwarded to
        // active subscribers in real time.
        OutputListener listener = subscriptions.get(streamName);
        if (listener != null) {
            listener.onMessage(streamName, timestamp, values);
        }

        // Propagate to all dependent views
        Set<String> dependents = dependencies.get(streamName);
        if (dependents != null) {
            for (String dependent : dependents) {
                List<OutputMessage> outputs = feedDependentPipeline(
                        dependent, streamName, timestamp, values);
                for (OutputMessage out : outputs) {
                    appendAndPropagate(dependent, out.timestamp, out.values);
                }
            }
        }
    }

    private Map<String, Long> parseLongCountersJson(String jsonText) {
        Map<String, Long> out = new LinkedHashMap<>();
        if (jsonText == null || jsonText.trim().isEmpty()) {
            return out;
        }
        com.google.gson.JsonObject root =
                gson.fromJson(jsonText, com.google.gson.JsonObject.class);
        if (root == null) {
            return out;
        }
        for (Map.Entry<String, com.google.gson.JsonElement> e : root.entrySet()) {
            com.google.gson.JsonElement value = e.getValue();
            if (value != null
                    && value.isJsonPrimitive()
                    && value.getAsJsonPrimitive().isNumber()) {
                out.put(e.getKey(), value.getAsLong());
            }
        }
        return out;
    }

    /**
     * Feed a message to a dependent pipeline on its assigned input port.
     *
     * <p>The compiled RTBot program handles all time synchronization internally.
     * The runtime simply routes each source's messages to the correct port.
     */
    private List<OutputMessage> feedDependentPipeline(String dependent, String streamName,
                                                      long timestamp, List<Double> values) {
        String pipelineId = viewPipelines.get(dependent);
        if (pipelineId == null) return Collections.emptyList();

        Map<String, String> portMap = viewPortMaps.getOrDefault(dependent, Collections.emptyMap());
        String port = portMap.getOrDefault(streamName, "i1");
        return runner.feed(pipelineId, timestamp, values, port);
    }

    // =================================================================
    // SELECT execution
    // =================================================================

    /**
     * Execute a tier-1 SELECT: direct read from materialized store.
     */
    private SelectResult executeTier1(String sql, CompilationResult result) {
        String source = (result.sourceStreams != null && !result.sourceStreams.isEmpty())
                ? result.sourceStreams.get(0) : "";
        if (source.isEmpty()) {
            return new SelectResult();
        }

        Map<String, Integer> effectiveFieldMap = resolveFieldMap(source, result.fieldMap);
        Integer limit = extractLimit(sql);
        ViewMeta sourceView = catalog.lookupView(source);

        List<Message> messages;

        if (sourceView != null
                && ViewType.KEYED.name().equals(sourceView.viewType)
                && !sql.toLowerCase().contains(" where ")) {
            // Keyed view without WHERE: read latest per key
            Set<Double> knownKeys = new TreeSet<>(catalog.getKnownKeys(source));
            Map<Double, Message> latestByKey = store.readLatestPerKey(
                    source, knownKeys, sourceView.keyIndex);
            messages = new ArrayList<>();
            for (Double key : knownKeys) {
                Message msg = latestByKey.get(key);
                if (msg != null) messages.add(msg);
            }
            if (limit != null && messages.size() > limit) {
                messages = messages.subList(0, limit);
            }
        } else {
            if (limit == null) {
                messages = store.read(source);
            } else {
                messages = store.readLatest(source, limit);
            }
        }

        List<List<Double>> rows = projectMessages(messages, effectiveFieldMap);
        List<Long> timestamps = new ArrayList<>();
        for (Message msg : messages) {
            timestamps.add(msg.timestamp);
        }

        List<String> columns = new ArrayList<>(effectiveFieldMap.keySet());
        columns.sort((a, b) -> Integer.compare(effectiveFieldMap.get(a), effectiveFieldMap.get(b)));

        return new SelectResult(columns, rows, timestamps);
    }

    /**
     * Execute a SELECT via ephemeral pipeline (tier-2 / tier-3).
     *
     * <p>Wraps the SELECT as "CREATE MATERIALIZED VIEW __rtbot_sql_tmp AS ..."
     * to get a program_json, then replays stored data through the pipeline.
     */
    private SelectResult executeWithPipeline(String sql, CompilationResult result) {
        Map<String, Integer> originalFieldMap = result.fieldMap != null
                ? new HashMap<>(result.fieldMap) : new HashMap<>();

        CompilationResult runtimeResult = result;
        if (runtimeResult.programJson == null || runtimeResult.programJson.isEmpty()) {
            runtimeResult = compileSelectToProgram(sql);
            if (runtimeResult.hasErrors()) {
                throw new SqlError(runtimeResult.errors);
            }
        }

        Map<String, Integer> effectiveFieldMap = runtimeResult.fieldMap != null
                ? new HashMap<>(runtimeResult.fieldMap) : new HashMap<>();
        if (effectiveFieldMap.isEmpty()) {
            effectiveFieldMap = new HashMap<>(originalFieldMap);
        }
        if (effectiveFieldMap.isEmpty() && runtimeResult.sourceStreams != null
                && !runtimeResult.sourceStreams.isEmpty()) {
            effectiveFieldMap = resolveFieldMap(runtimeResult.sourceStreams.get(0), null);
        }
        // Capture as effectively-final for use in lambdas
        final Map<String, Integer> fieldMap = effectiveFieldMap;

        if (runtimeResult.sourceStreams == null || runtimeResult.sourceStreams.isEmpty()) {
            List<String> columns = new ArrayList<>(fieldMap.keySet());
            columns.sort((a, b) -> Integer.compare(fieldMap.get(a), fieldMap.get(b)));
            return new SelectResult(columns, new ArrayList<>(), new ArrayList<>());
        }

        // Build input messages — interleaved by timestamp for correct ordering
        List<InputMessage> inputs = new ArrayList<>();
        for (int i = 0; i < runtimeResult.sourceStreams.size(); i++) {
            String source = runtimeResult.sourceStreams.get(i);
            String port = "i" + (i + 1);
            for (Message msg : store.read(source)) {
                inputs.add(new InputMessage(msg.timestamp, new ArrayList<>(msg.values), port));
            }
        }
        inputs.sort((a, b) -> Long.compare(a.timestamp, b.timestamp));

        // Run through ephemeral pipeline
        List<OutputMessage> outputs = runner.runOnce(runtimeResult.programJson, inputs);
        List<Long> timestamps = new ArrayList<>();
        List<List<Double>> rows = new ArrayList<>();
        for (OutputMessage out : outputs) {
            timestamps.add(out.timestamp);
            rows.add(new ArrayList<>(out.values));
        }

        // Apply LIMIT
        Integer limit = extractLimit(sql);
        if (limit != null && rows.size() > limit) {
            rows = rows.subList(0, limit);
            timestamps = timestamps.subList(0, limit);
        }

        List<String> columns = new ArrayList<>(fieldMap.keySet());
        columns.sort((a, b) -> Integer.compare(fieldMap.get(a), fieldMap.get(b)));

        return new SelectResult(columns, rows, timestamps);
    }

    /**
     * Compile a SELECT by wrapping it as CREATE MATERIALIZED VIEW to get program_json.
     */
    private CompilationResult compileSelectToProgram(String sql) {
        String selectSql = sql.strip();
        if (selectSql.endsWith(";")) {
            selectSql = selectSql.substring(0, selectSql.length() - 1);
        }
        String wrappedSql = "CREATE MATERIALIZED VIEW __rtbot_sql_tmp AS " + selectSql;
        String catalogJson = catalog.snapshotJson();
        String resultJson = RtBotSqlCompiler.compileSqlJson(wrappedSql, catalogJson, tsUnitsPerSecond);

        com.google.gson.JsonObject wrapper = gson.fromJson(resultJson, com.google.gson.JsonObject.class);
        com.google.gson.JsonArray results = wrapper.getAsJsonArray("results");
        if (results.size() == 0) {
            CompilationResult empty = new CompilationResult();
            empty.errors = new java.util.ArrayList<>();
            empty.errors.add(new CompilationError("No compilation results", -1, -1));
            return empty;
        }
        // Return the last result (the CREATE MATERIALIZED VIEW)
        return gson.fromJson(results.get(results.size() - 1), CompilationResult.class);
    }

    // =================================================================
    // Projection
    // =================================================================

    /**
     * Project messages through a field map, producing value rows in field-map order.
     */
    private List<List<Double>> projectMessages(List<Message> messages, Map<String, Integer> fieldMap) {
        // Sort fields by their index to determine column ordering
        List<Map.Entry<String, Integer>> ordered = new ArrayList<>(fieldMap.entrySet());
        ordered.sort(Map.Entry.comparingByValue());
        int[] indices = new int[ordered.size()];
        for (int i = 0; i < ordered.size(); i++) {
            indices[i] = ordered.get(i).getValue();
        }

        List<List<Double>> rows = new ArrayList<>();
        for (Message msg : messages) {
            List<Double> row = new ArrayList<>();
            for (int idx : indices) {
                if (idx >= 0 && idx < msg.values.size()) {
                    row.add(msg.values.get(idx));
                } else {
                    row.add(0.0);
                }
            }
            rows.add(row);
        }
        return rows;
    }

    /**
     * Resolve an effective field map for a source, using the compilation result's
     * field map if available, otherwise falling back to catalog metadata.
     */
    private Map<String, Integer> resolveFieldMap(String source, Map<String, Integer> fieldMap) {
        if (fieldMap != null && !fieldMap.isEmpty()) {
            return new HashMap<>(fieldMap);
        }

        ViewMeta sourceView = catalog.lookupView(source);
        if (sourceView != null) {
            return new HashMap<>(sourceView.fieldMap);
        }

        StreamSchema sourceStream = catalog.lookupStream(source);
        if (sourceStream != null) {
            Map<String, Integer> result = new LinkedHashMap<>();
            for (ColumnDef col : sourceStream.columns) {
                result.put(col.name, col.index);
            }
            return result;
        }

        TableSchema sourceTable = catalog.lookupTable(source);
        if (sourceTable != null) {
            Map<String, Integer> result = new LinkedHashMap<>();
            for (ColumnDef col : sourceTable.columns) {
                result.put(col.name, col.index);
            }
            return result;
        }

        return new HashMap<>();
    }

    // =================================================================
    // Utilities
    // =================================================================

    /**
     * Extract LIMIT value from a SQL string, or {@code null} if not present.
     */
    private static Integer extractLimit(String sql) {
        Matcher matcher = LIMIT_PATTERN.matcher(sql);
        if (!matcher.find()) return null;
        return Integer.parseInt(matcher.group(1));
    }

    /**
     * Generate the next monotonically increasing timestamp (milliseconds).
     */
    private long nextTimestamp() {
        long now = System.currentTimeMillis();
        if (now <= lastTimestamp) {
            lastTimestamp++;
        } else {
            lastTimestamp = now;
        }
        return lastTimestamp;
    }

    /**
     * Serialize the full runtime state (pipelines and dictionaries) for persistence.
     *
     * <p>Pipeline states are keyed by view name. Dictionary states are keyed by
     * {@code "dict:" + dictKey} (e.g. {@code "dict:sensors.location"}) with JSON
     * values from {@link StringDictionary#toJson()}.
     *
     * @return a map of state keys to serialized JSON values
     */
    public Map<String, String> serializeState() {
        Map<String, String> state = new HashMap<>();
        for (Map.Entry<String, String> entry : viewPipelines.entrySet()) {
            String pipelineId = entry.getValue();
            try {
                state.put(entry.getKey(), runner.serialize(pipelineId));
            } catch (Exception e) {
                // Skip pipelines that fail to serialize
            }
        }

        // Serialize dictionaries alongside pipeline state
        for (Map.Entry<String, StringDictionary> entry : catalog.getDictionaries().entrySet()) {
            if (!entry.getValue().isEmpty()) {
                state.put("dict:" + entry.getKey(), entry.getValue().toJson());
            }
        }

        return state;
    }

    /**
     * Restore pipeline and dictionary state from a previously serialized state map.
     *
     * <p>Entries with keys prefixed by {@code "dict:"} are restored as
     * {@link StringDictionary} instances in the catalog. All other entries
     * are treated as pipeline state keyed by view name.
     *
     * @param state map of state keys to serialized JSON values
     */
    public void restoreState(Map<String, String> state) {
        for (Map.Entry<String, String> entry : state.entrySet()) {
            if (entry.getKey().startsWith("dict:")) {
                // Restore dictionary
                String dictKey = entry.getKey().substring(5);
                StringDictionary dict = StringDictionary.fromJson(entry.getValue());
                catalog.registerDictionary(dictKey, dict);
            } else {
                // Restore pipeline state
                String viewName = entry.getKey();
                String pipelineId = viewPipelines.get(viewName);
                if (pipelineId != null) {
                    runner.restore(pipelineId, entry.getValue());
                }
            }
        }
    }

    /**
     * Destroy all deployed pipelines, clear subscriptions, and reset state.
     */
    public void destroyAll() {
        for (String pipelineId : viewPipelines.values()) {
            try {
                runner.destroy(pipelineId);
            } catch (Exception e) {
                // Best effort cleanup
            }
        }
        viewPipelines.clear();
        viewPortMaps.clear();
        dependencies.clear();
        subscriptions.clear();
    }
}

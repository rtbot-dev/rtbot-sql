package dev.rtbot.sql;

import com.google.gson.Gson;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
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

    // -- Consolidated-session state --------------------------------------
    // All registered views compile into a single rtbot Program at first
    // insert. Outputs are demuxed by upstream operator id to each view
    // terminal (plain and materialized alike so subscribers on plain
    // views still get notified). DDL invalidates the session so the
    // next insert rebuilds it from the updated catalog.
    private String sessionPipelineId = null;
    /** Preresolved view info indexed by op_index registered with the JNI. */
    private SessionViewInfo[] sessionTerminalsByIndex = new SessionViewInfo[0];
    /**
     * Side-index keyed by operator id string. Used only by the JSON-output
     * fallback paths ({@link #feedSessionRow3}, {@link #feedSessionRow},
     * non-"i1"-port sessions). Hot path uses the index array.
     */
    private Map<String, SessionViewInfo> sessionOpIdToInfo = Collections.emptyMap();
    /** base stream name -> port id on the session Input */
    private Map<String, String> sessionStreamPort = Collections.emptyMap();
    /**
     * Reusable direct buffer for {@link #feedSessionBuffer} outputs.
     * Allocated lazily at first session deploy and grown on demand when
     * the native side reports insufficient capacity.
     */
    private ByteBuffer sessionOutBuf = null;
    private static final int SESSION_OUT_BUF_INITIAL_CAPACITY = 64 * 1024;

    /**
     * Preresolved view metadata keyed by the merged-graph terminal operator
     * id. Built once at {@link #ensureSessionDeployed} so the output
     * dispatch hot path avoids repeated {@link InMemoryCatalog#lookupView}
     * calls and HashMap churn for every emitted message.
     */
    private static final class SessionViewInfo {
        final String viewName;
        final boolean storeOutput;   // true for MATERIALIZED_VIEW (not plain VIEW)
        final boolean keyed;         // true for KEYED view_type with valid keyIndex
        final int keyIndex;          // -1 if not keyed

        SessionViewInfo(String viewName, boolean storeOutput,
                         boolean keyed, int keyIndex) {
            this.viewName = viewName;
            this.storeOutput = storeOutput;
            this.keyed = keyed;
            this.keyIndex = keyIndex;
        }
    }

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
     * Encodes a text value to its dictionary double for a TEXT column. The
     * mapping is stable — the same string always returns the same double
     * within this runtime instance. New strings are assigned the next
     * sequential ID automatically.
     *
     * <p>This is a pure Java {@link java.util.HashMap} lookup (~20ns) with
     * no JNI crossing, so it is safe to call per-row when building
     * all-double batch arrays for {@link #insertBatch}.
     */
    public double encodeText(String streamName, String columnName, String textValue) {
        String dictKey = streamName + "." + columnName;
        return catalog.getOrCreateDictionary(dictKey).encode(textValue);
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
        int expectedCols = -1;
        if (schema != null) {
            expectedCols = schema.columns == null ? -1 : schema.columns.size();
        } else {
            TableSchema table = catalog.lookupTable(streamName);
            if (table == null) {
                throw new SqlError("Unknown stream or table: " + streamName);
            }
            expectedCols = table.columns == null ? -1 : table.columns.size();
        }

        List<Double> payload = new ArrayList<>(values);
        if (expectedCols > 0 && payload.size() != expectedCols) {
            throw new SqlError("INSERT payload length mismatch for " + streamName
                    + ": expected " + expectedCols + ", got " + payload.size());
        }

        ensureSessionDeployed();
        appendAndPropagate(streamName, timestamp, payload);
        feedSessionRow(streamName, timestamp, payload);
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

        ensureSessionDeployed();
        if (collectMode || subscriptions.containsKey(streamName)) {
            appendAndPropagate(streamName, timestamp,
                                Arrays.asList(v0, v1, v2));
        }
        feedSessionRow3(streamName, timestamp, v0, v1, v2);
    }

    /**
     * Batched insert path for numeric streams with any number of columns.
     *
     * <p>{@code columns} is column-major: {@code columns[c][i]} is the value of
     * column {@code c} at row {@code i}. Feeds the whole batch in one native
     * call for single-source dependents on port {@code i1}. Falls back to
     * row-by-row routing when collect mode is enabled, the source has direct
     * subscribers, or a dependent requires a non-default input port.
     */
    /**
     * Raw-buffer variant of {@link #insertBatch}. Same contract, but routes
     * i1 dependents through {@code feedBufferI1} so operators that override
     * {@code receive_data_buffer} (e.g. BurstAggregate) skip per-row Message
     * allocation on the native side. Non-i1 ports and subscriber/collect
     * modes fall back to row-by-row routing.
     */
    public void insertBuffer(String streamName,
                              long[] timestamps,
                              double[][] columns) {
        insertBatchImpl(streamName, timestamps, columns, /*useBuffer=*/true);
    }

    public void insertBatch(String streamName,
                            long[] timestamps,
                            double[][] columns) {
        insertBatchImpl(streamName, timestamps, columns, /*useBuffer=*/false);
    }

    private void insertBatchImpl(String streamName,
                                   long[] timestamps,
                                   double[][] columns,
                                   boolean useBuffer) {
        StreamSchema schema = catalog.lookupStream(streamName);
        if (schema == null) {
            throw new SqlError("Unknown stream: " + streamName);
        }
        if (timestamps == null || columns == null) {
            throw new SqlError("insertBatch arrays must not be null");
        }
        if (schema.columns != null && !schema.columns.isEmpty()
                && schema.columns.size() != columns.length) {
            throw new SqlError("insertBatch column count mismatch for " + streamName
                    + ": schema=" + schema.columns.size()
                    + ", got=" + columns.length);
        }
        int n = timestamps.length;
        for (int c = 0; c < columns.length; c++) {
            if (columns[c] == null || columns[c].length != n) {
                throw new SqlError("insertBatch column " + c + " null or length mismatch for "
                        + streamName + ": expected " + n);
            }
        }
        if (n == 0) {
            return;
        }

        ensureSessionDeployed();
        if (collectMode || subscriptions.containsKey(streamName)) {
            for (int i = 0; i < n; i++) {
                List<Double> row = new ArrayList<>(columns.length);
                for (double[] col : columns) row.add(col[i]);
                appendAndPropagate(streamName, timestamps[i], row);
            }
        }
        feedSessionBuffer(streamName, timestamps, columns, useBuffer);
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
     * Lazily compile and deploy the consolidated session.
     * Throws {@link SqlError} if the catalog cannot be represented as a
     * single Program (e.g. cycle, unknown source).
     */
    private void ensureSessionDeployed() {
        if (sessionPipelineId != null) return;
        if (catalog.getViews().isEmpty()) return;  // no views → nothing to deploy
        String catalogJson = catalog.snapshotJson();
        String sessionJson = RtBotSqlCompiler.compileSessionJson(catalogJson);
        com.google.gson.JsonObject obj =
                gson.fromJson(sessionJson, com.google.gson.JsonObject.class);
        com.google.gson.JsonArray errs = obj.getAsJsonArray("errors");
        if (errs != null && errs.size() > 0) {
            throw new SqlError("compile_session failed: " + errs.toString());
        }
        String programJson = obj.get("program_json").getAsString();
        sessionPipelineId = runner.deploy(programJson, new ArrayList<>(),
                                           Collections.emptyMap());

        // Build the opIdx-aligned terminal table and register the
        // ordering with the JNI so native outputs reference indices
        // (not strings) in the binary output frame.
        com.google.gson.JsonObject vt = obj.getAsJsonObject("view_terminals");
        int count = vt == null ? 0 : vt.size();
        String[] opIds = new String[count];
        SessionViewInfo[] infos = new SessionViewInfo[count];
        Map<String, SessionViewInfo> byId = new HashMap<>(count * 2);
        int idx = 0;
        for (Map.Entry<String, com.google.gson.JsonElement> e : vt.entrySet()) {
            String viewName = e.getKey();
            String opId = e.getValue().getAsString();
            ViewMeta meta = catalog.lookupView(viewName);
            boolean storeOutput = meta != null
                    && EntityType.MATERIALIZED_VIEW.name().equals(meta.entityType);
            boolean keyed = meta != null
                    && ViewType.KEYED.name().equals(meta.viewType)
                    && meta.keyIndex >= 0;
            int keyIndex = keyed ? meta.keyIndex : -1;
            SessionViewInfo info = new SessionViewInfo(
                    viewName, storeOutput, keyed, keyIndex);
            opIds[idx] = opId;
            infos[idx] = info;
            byId.put(opId, info);
            idx++;
        }
        runner.registerSessionOutputs(sessionPipelineId, opIds);
        this.sessionTerminalsByIndex = infos;
        this.sessionOpIdToInfo = byId;

        // Allocate the reusable output buffer once.
        if (sessionOutBuf == null) {
            sessionOutBuf = ByteBuffer
                    .allocateDirect(SESSION_OUT_BUF_INITIAL_CAPACITY)
                    .order(ByteOrder.nativeOrder());
        }

        // Build base_stream -> port map for routing.
        Map<String, String> sp = new HashMap<>();
        com.google.gson.JsonObject bsp =
                obj.getAsJsonObject("base_stream_ports");
        for (Map.Entry<String, com.google.gson.JsonElement> e : bsp.entrySet()) {
            sp.put(e.getKey(), e.getValue().getAsString());
        }
        this.sessionStreamPort = sp;

        // Backfill: replay all stored base-stream messages through the
        // fresh session in global timestamp order so a newly built
        // session picks up the state the per-view pipelines already had
        // from their own backfillInterleaved (e.g. after DROP+recreate
        // or when a view is created after data has been inserted).
        backfillSessionFromStore();
    }

    private void backfillSessionFromStore() {
        // Collect stored messages for each base stream with its port.
        List<long[]> eventKeys = new ArrayList<>();   // [timestamp, streamIndex]
        List<String> eventPorts = new ArrayList<>();
        List<List<Double>> eventValues = new ArrayList<>();

        int streamIdx = 0;
        for (Map.Entry<String, String> entry : sessionStreamPort.entrySet()) {
            String stream = entry.getKey();
            String port = entry.getValue();
            for (Message msg : store.read(stream)) {
                eventKeys.add(new long[]{msg.timestamp, streamIdx});
                eventPorts.add(port);
                eventValues.add(new ArrayList<>(msg.values));
            }
            streamIdx++;
        }
        if (eventKeys.isEmpty()) return;

        // Sort by (timestamp, streamIndex) for deterministic replay.
        Integer[] indices = new Integer[eventKeys.size()];
        for (int i = 0; i < indices.length; i++) indices[i] = i;
        java.util.Arrays.sort(indices, (a, b) -> {
            long[] ea = eventKeys.get(a);
            long[] eb = eventKeys.get(b);
            if (ea[0] != eb[0]) return Long.compare(ea[0], eb[0]);
            return Long.compare(ea[1], eb[1]);
        });

        for (int idx : indices) {
            long ts = eventKeys.get(idx)[0];
            String port = eventPorts.get(idx);
            List<Double> values = eventValues.get(idx);
            // Single-row feed; dispatch outputs so materialized views
            // populate the store and subscribers receive messages just
            // as they would have if the view had existed at ingest time.
            List<OutputMessage> outputs =
                    runner.feed(sessionPipelineId, ts, values, port);
            dispatchSessionOutputs(outputs);
        }
    }

    /**
     * Feed a batch into the consolidated session on the given base
     * stream's port, then demux outputs to each view terminal's
     * subscribers / materialized-view store via the binary frame
     * returned by {@link RtBotSqlCompiler#feedPipelineBufferSession}.
     */
    private void feedSessionBuffer(String streamName, long[] timestamps,
                                    double[][] columns, boolean useBuffer) {
        if (sessionPipelineId == null) return;
        String port = sessionStreamPort.getOrDefault(streamName, "i1");
        int bytes = runner.feedBufferSession(
                sessionPipelineId, port, timestamps, columns, sessionOutBuf);
        if (bytes < -1) {
            int needed = -bytes;
            int newCap = Math.max(needed, sessionOutBuf.capacity() * 2);
            sessionOutBuf = ByteBuffer.allocateDirect(newCap)
                    .order(ByteOrder.nativeOrder());
            bytes = runner.feedBufferSession(
                    sessionPipelineId, port, timestamps, columns, sessionOutBuf);
        }
        if (bytes < 0) {
            throw new SqlError(
                    "feedBufferSession failed with return code " + bytes);
        }
        dispatchSessionBinary(sessionOutBuf, bytes);
    }

    private void feedSessionRow3(String streamName, long timestamp,
                                   double v0, double v1, double v2) {
        if (sessionPipelineId == null) return;
        String port = sessionStreamPort.getOrDefault(streamName, "i1");
        dispatchSessionOutputs(
                runner.feed3(sessionPipelineId, timestamp, v0, v1, v2, port));
    }

    private void feedSessionRow(String streamName, long timestamp,
                                  List<Double> values) {
        if (sessionPipelineId == null) return;
        String port = sessionStreamPort.getOrDefault(streamName, "i1");
        dispatchSessionOutputs(
                runner.feed(sessionPipelineId, timestamp, values, port));
    }

    /**
     * JSON-output dispatch used by the per-row insert paths
     * ({@link #insert3} / {@link #insert}). The buffered path uses the
     * index-keyed binary frame via {@link #dispatchSessionBinary}.
     */
    private void dispatchSessionOutputs(List<OutputMessage> outputs) {
        for (OutputMessage out : outputs) {
            SessionViewInfo info = sessionOpIdToInfo.get(out.operatorId);
            if (info == null) continue;
            applySessionOutput(info, out.timestamp, out.values);
        }
    }

    /**
     * Parse the binary output frame written by
     * {@link RtBotSqlCompiler#feedPipelineBufferSession} and dispatch
     * each message using the preregistered op-index table. No POJO
     * allocation, no Gson, no string keys — just {@link ByteBuffer} reads
     * plus one array index per message.
     */
    private void dispatchSessionBinary(ByteBuffer buf, int bytes) {
        buf.order(ByteOrder.nativeOrder()).position(0).limit(bytes);
        int nOut = buf.getInt();
        for (int m = 0; m < nOut; m++) {
            int opIdx = buf.getInt();
            int nVals = buf.getInt();
            long ts = buf.getLong();
            List<Double> values = new ArrayList<>(nVals);
            for (int v = 0; v < nVals; v++) values.add(buf.getDouble());
            if (opIdx < 0 || opIdx >= sessionTerminalsByIndex.length) continue;
            SessionViewInfo info = sessionTerminalsByIndex[opIdx];
            if (info == null) continue;
            applySessionOutput(info, ts, values);
        }
    }

    private void applySessionOutput(SessionViewInfo info, long timestamp,
                                     List<Double> values) {
        if (info.storeOutput && collectMode) {
            store.append(info.viewName, timestamp, values);
            if (info.keyed && info.keyIndex < values.size()) {
                catalog.addKey(info.viewName, values.get(info.keyIndex));
            }
        }
        OutputListener listener = subscriptions.get(info.viewName);
        if (listener != null) {
            listener.onMessage(info.viewName, timestamp, values);
        }
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

        insert(streamName, nextTimestamp(), payload);
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

        // Invalidate and eagerly redeploy the consolidated session so
        // the new view participates from this moment (and its state
        // picks up any already-stored source data via
        // `backfillSessionFromStore`). Matching the legacy
        // `backfillInterleaved` behaviour means
        // `CREATE VIEW ... FROM stream` after
        // `INSERT INTO stream` populates the view immediately.
        invalidateSession();
        ensureSessionDeployed();
    }

    private void invalidateSession() {
        if (sessionPipelineId != null) {
            try { runner.destroy(sessionPipelineId); } catch (Exception ignored) {}
            sessionPipelineId = null;
        }
        sessionTerminalsByIndex = new SessionViewInfo[0];
        sessionOpIdToInfo = Collections.emptyMap();
        sessionStreamPort = Collections.emptyMap();
    }

    /**
     * Handle DROP: remove the entity from the catalog + store and
     * invalidate the session so the next insert rebuilds it.
     */
    private void handleDrop(CompilationResult result) {
        String name = result.dropEntityName;
        catalog.drop(name);
        store.clear(name);

        // Catalog changed — force a session rebuild on the next insert.
        invalidateSession();
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
     * Per-message side effects for an inserted stream row: store under
     * collect mode, notify subscribers. Propagation to dependent views
     * is NOT the caller's responsibility — the consolidated session
     * Program routes data between views internally and dispatches each
     * materialized output through {@link #applySessionOutput}.
     */
    private void appendAndPropagate(String streamName, long timestamp,
                                     List<Double> values) {
        if (collectMode) {
            store.append(streamName, timestamp, values);
        }
        OutputListener listener = subscriptions.get(streamName);
        if (listener != null) {
            listener.onMessage(streamName, timestamp, values);
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
    /** Key under which the consolidated-session pipeline state is stored. */
    public static final String SESSION_STATE_KEY = "__session__";

    public Map<String, String> serializeState() {
        Map<String, String> state = new HashMap<>();
        // When the consolidated session owns the live state (it handled
        // the inserts), that single blob is authoritative. Per-view
        // pipelines in session mode are never fed, so their state is
        // only useful in the fallback path.
        if (sessionPipelineId != null) {
            try {
                state.put(SESSION_STATE_KEY, runner.serialize(sessionPipelineId));
            } catch (Exception ignored) {
                // Skip on failure
            }
        }
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
            String key = entry.getKey();
            if (key.equals(SESSION_STATE_KEY)) continue;  // handled below
            if (key.startsWith("dict:")) {
                String dictKey = key.substring(5);
                StringDictionary dict = StringDictionary.fromJson(entry.getValue());
                catalog.registerDictionary(dictKey, dict);
            }
        }

        String sessionBlob = state.get(SESSION_STATE_KEY);
        if (sessionBlob != null) {
            invalidateSession();
            ensureSessionDeployed();
            if (sessionPipelineId != null) {
                try {
                    runner.restore(sessionPipelineId, sessionBlob);
                } catch (Exception ignored) {
                }
            }
        }
    }

    /**
     * Destroy all deployed pipelines, clear subscriptions, and reset state.
     */
    public void destroyAll() {
        subscriptions.clear();
        if (sessionPipelineId != null) {
            try { runner.destroy(sessionPipelineId); } catch (Exception ignored) {}
            sessionPipelineId = null;
            sessionTerminalsByIndex = new SessionViewInfo[0];
            sessionOpIdToInfo = Collections.emptyMap();
            sessionStreamPort = Collections.emptyMap();
        }
    }
}

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

    private long lastTimestamp = System.currentTimeMillis();
    private final Gson gson = new Gson();

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
        String normalized = normalizeSql(sql);
        String catalogJson = catalog.snapshotJson();
        String resultJson = RtBotSqlCompiler.compileSqlJson(normalized, catalogJson);
        CompilationResult result = gson.fromJson(resultJson, CompilationResult.class);

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
                return null;
            case INSERT:
                handleInsert(result);
                return null;
            case CREATE_VIEW:
            case CREATE_MATERIALIZED_VIEW:
                handleCreateView(result);
                return null;
            case DROP:
                handleDrop(result);
                return null;
            case SELECT:
                return handleSelect(normalized, result);
            default:
                throw new SqlError("Unsupported statement type: " + result.statementType);
        }
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

        // Backfill the new view from already-known source data
        if (usesSnapshotSync(name)) {
            backfillWithSnapshotSync(name, viewMeta, sourcePortMap, pipelineId);
        } else {
            backfillSimple(name, viewMeta, sourcePortMap, pipelineId);
        }
    }

    /**
     * Backfill a view using ASOF snapshot synchronization for multi-source views.
     *
     * <p>Replays all source events in timestamp order. At each event, builds a
     * snapshot from all sources (using the latest value at or before the event
     * timestamp for non-trigger sources). Only feeds when all sources have data.
     */
    private void backfillWithSnapshotSync(String name, ViewMeta viewMeta,
                                          Map<String, String> sourcePortMap, String pipelineId) {
        // Assign rank to each source for deterministic ordering at same timestamp
        Map<String, Integer> sourceRank = new HashMap<>();
        for (int i = 0; i < viewMeta.sourceStreams.size(); i++) {
            sourceRank.put(viewMeta.sourceStreams.get(i), i);
        }

        // Collect all events from all sources
        List<long[]> eventTimestamps = new ArrayList<>(); // [timestamp, rank]
        List<String> eventSources = new ArrayList<>();
        List<List<Double>> eventValues = new ArrayList<>();

        for (String source : viewMeta.sourceStreams) {
            int rank = sourceRank.get(source);
            for (Message msg : store.read(source)) {
                eventTimestamps.add(new long[]{msg.timestamp, rank});
                eventSources.add(source);
                eventValues.add(new ArrayList<>(msg.values));
            }
        }

        // Sort by (timestamp, rank)
        Integer[] indices = new Integer[eventTimestamps.size()];
        for (int i = 0; i < indices.length; i++) indices[i] = i;
        java.util.Arrays.sort(indices, (a, b) -> {
            long[] ea = eventTimestamps.get(a);
            long[] eb = eventTimestamps.get(b);
            if (ea[0] != eb[0]) return Long.compare(ea[0], eb[0]);
            return Long.compare(ea[1], eb[1]);
        });

        // Replay in order, building history and snapshots
        // history: source -> list of (timestamp, values)
        Map<String, List<long[]>> historyTimestamps = new HashMap<>();
        Map<String, List<List<Double>>> historyValues = new HashMap<>();
        for (String source : viewMeta.sourceStreams) {
            historyTimestamps.put(source, new ArrayList<>());
            historyValues.put(source, new ArrayList<>());
        }

        for (int idx : indices) {
            long timestamp = eventTimestamps.get(idx)[0];
            String triggerSource = eventSources.get(idx);
            List<Double> triggerValues = eventValues.get(idx);

            // Append to history
            historyTimestamps.get(triggerSource).add(new long[]{timestamp});
            historyValues.get(triggerSource).add(new ArrayList<>(triggerValues));

            // Build snapshot: for each source, find the latest values at or before timestamp
            Map<String, List<Double>> snapshot = new LinkedHashMap<>();
            boolean ready = true;

            for (String source : viewMeta.sourceStreams) {
                if (source.equals(triggerSource)) {
                    snapshot.put(source, new ArrayList<>(triggerValues));
                    continue;
                }

                // Find latest prior value from history
                List<long[]> srcTimestamps = historyTimestamps.get(source);
                List<List<Double>> srcValues = historyValues.get(source);
                List<Double> prior = null;

                for (int j = srcTimestamps.size() - 1; j >= 0; j--) {
                    if (srcTimestamps.get(j)[0] <= timestamp) {
                        prior = srcValues.get(j);
                        break;
                    }
                }

                if (prior == null) {
                    ready = false;
                    break;
                }

                snapshot.put(source, new ArrayList<>(prior));
            }

            if (!ready) continue;

            // Feed all sources in order to the pipeline
            List<OutputMessage> outputs = new ArrayList<>();
            for (String source : viewMeta.sourceStreams) {
                String port = sourcePortMap.getOrDefault(source, "i1");
                outputs.addAll(runner.feed(pipelineId, timestamp, snapshot.get(source), port));
            }

            for (OutputMessage out : outputs) {
                appendAndPropagate(name, out.timestamp, out.values);
            }
        }
    }

    /**
     * Simple backfill: replay each source's messages sequentially.
     */
    private void backfillSimple(String name, ViewMeta viewMeta,
                                Map<String, String> sourcePortMap, String pipelineId) {
        for (int i = 0; i < viewMeta.sourceStreams.size(); i++) {
            String source = viewMeta.sourceStreams.get(i);
            String port = "i" + (i + 1);
            for (Message msg : store.read(source)) {
                List<OutputMessage> outputs = runner.feed(pipelineId, msg.timestamp, msg.values, port);
                for (OutputMessage out : outputs) {
                    appendAndPropagate(name, out.timestamp, out.values);
                }
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
     * Append a message to a stream/view and propagate to all dependents.
     *
     * <p>Plain VIEWs (non-materialized) do NOT store output — they only propagate
     * to dependents. Only streams and MATERIALIZED VIEWs persist their output.
     */
    private void appendAndPropagate(String streamName, long timestamp, List<Double> values) {
        ViewMeta view = catalog.lookupView(streamName);
        boolean isPlainView = (view != null
                && EntityType.VIEW.name().equals(view.entityType));

        if (!isPlainView) {
            store.append(streamName, timestamp, values);

            // Track keys for keyed views
            if (view != null
                    && ViewType.KEYED.name().equals(view.viewType)
                    && view.keyIndex >= 0
                    && view.keyIndex < values.size()) {
                catalog.addKey(streamName, values.get(view.keyIndex));
            }
        }

        // Propagate to all dependent views
        Set<String> dependents = dependencies.get(streamName);
        if (dependents != null) {
            for (String dependent : new ArrayList<>(dependents)) {
                List<OutputMessage> outputs = feedDependentPipeline(
                        dependent, streamName, timestamp, values);
                for (OutputMessage out : outputs) {
                    appendAndPropagate(dependent, out.timestamp, out.values);
                }
            }
        }
    }

    /**
     * Feed a message to a dependent pipeline, using ASOF snapshot sync for
     * multi-source views.
     */
    private List<OutputMessage> feedDependentPipeline(String dependent, String streamName,
                                                      long timestamp, List<Double> values) {
        String pipelineId = viewPipelines.get(dependent);
        if (pipelineId == null) return Collections.emptyList();

        Map<String, String> portMap = viewPortMaps.getOrDefault(dependent, Collections.emptyMap());

        // Simple case: single source or non-stream sources
        if (!usesSnapshotSync(dependent)) {
            String port = portMap.getOrDefault(streamName, "i1");
            return runner.feed(pipelineId, timestamp, values, port);
        }

        // ASOF snapshot sync for multi-source views
        ViewMeta view = catalog.lookupView(dependent);
        if (view == null) return Collections.emptyList();

        Map<String, List<Double>> snapshot = new LinkedHashMap<>();
        for (String source : view.sourceStreams) {
            if (source.equals(streamName)) {
                snapshot.put(source, new ArrayList<>(values));
                continue;
            }

            Message prior = store.readLatestBefore(source, timestamp);
            if (prior == null) {
                // Not all sources have data yet — skip
                return Collections.emptyList();
            }
            snapshot.put(source, new ArrayList<>(prior.values));
        }

        // Feed all sources in order
        List<OutputMessage> outputs = new ArrayList<>();
        for (String source : view.sourceStreams) {
            String port = portMap.getOrDefault(source, "i1");
            outputs.addAll(runner.feed(pipelineId, timestamp, snapshot.get(source), port));
        }
        return outputs;
    }

    /**
     * Check if a dependent view uses ASOF snapshot synchronization.
     *
     * <p>True when the view has more than one source stream and all sources
     * are registered streams (not views or tables).
     */
    private boolean usesSnapshotSync(String dependent) {
        ViewMeta view = catalog.lookupView(dependent);
        if (view == null || view.sourceStreams.size() <= 1) return false;

        for (String source : view.sourceStreams) {
            if (catalog.lookupStream(source) == null) return false;
        }
        return true;
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

        // Build input messages
        List<InputMessage> inputs = new ArrayList<>();
        boolean useSnapshotSync = runtimeResult.sourceStreams.size() > 1
                && runtimeResult.sourceStreams.stream()
                        .allMatch(s -> catalog.lookupStream(s) != null);

        if (useSnapshotSync) {
            buildSnapshotSyncInputs(runtimeResult, inputs);
        } else {
            // Simple: feed each source's messages sequentially
            for (int i = 0; i < runtimeResult.sourceStreams.size(); i++) {
                String source = runtimeResult.sourceStreams.get(i);
                String port = "i" + (i + 1);
                for (Message msg : store.read(source)) {
                    inputs.add(new InputMessage(msg.timestamp, new ArrayList<>(msg.values), port));
                }
            }
        }

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
     * Build ASOF snapshot-synchronized inputs for a multi-source ephemeral query.
     */
    private void buildSnapshotSyncInputs(CompilationResult runtimeResult, List<InputMessage> inputs) {
        // Assign rank to each source for deterministic ordering at same timestamp
        Map<String, Integer> sourceRank = new HashMap<>();
        for (int i = 0; i < runtimeResult.sourceStreams.size(); i++) {
            sourceRank.put(runtimeResult.sourceStreams.get(i), i);
        }

        // Collect all events
        List<long[]> eventKeys = new ArrayList<>(); // [timestamp, rank]
        List<String> eventSources = new ArrayList<>();
        List<List<Double>> eventValues = new ArrayList<>();

        for (String source : runtimeResult.sourceStreams) {
            int rank = sourceRank.get(source);
            for (Message msg : store.read(source)) {
                eventKeys.add(new long[]{msg.timestamp, rank});
                eventSources.add(source);
                eventValues.add(new ArrayList<>(msg.values));
            }
        }

        // Sort by (timestamp, rank)
        Integer[] sortedIndices = new Integer[eventKeys.size()];
        for (int i = 0; i < sortedIndices.length; i++) sortedIndices[i] = i;
        java.util.Arrays.sort(sortedIndices, (a, b) -> {
            long[] ea = eventKeys.get(a);
            long[] eb = eventKeys.get(b);
            if (ea[0] != eb[0]) return Long.compare(ea[0], eb[0]);
            return Long.compare(ea[1], eb[1]);
        });

        // Build snapshots: latest value per source
        Map<String, List<Double>> latestBySource = new HashMap<>();

        for (int idx : sortedIndices) {
            long timestamp = eventKeys.get(idx)[0];
            String triggerSource = eventSources.get(idx);
            List<Double> triggerValues = eventValues.get(idx);

            latestBySource.put(triggerSource, new ArrayList<>(triggerValues));

            // Only produce inputs when all sources have data
            if (latestBySource.size() < runtimeResult.sourceStreams.size()) {
                continue;
            }

            // Emit one input per source (in source order)
            for (int i = 0; i < runtimeResult.sourceStreams.size(); i++) {
                String source = runtimeResult.sourceStreams.get(i);
                inputs.add(new InputMessage(
                        timestamp,
                        new ArrayList<>(latestBySource.get(source)),
                        "i" + (i + 1)
                ));
            }
        }
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
        String resultJson = RtBotSqlCompiler.compileSqlJson(wrappedSql, catalogJson);
        return gson.fromJson(resultJson, CompilationResult.class);
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
     * Serialize the full runtime state (pipelines) for persistence.
     *
     * @return a map of pipeline ID to serialized JSON state
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
        return state;
    }

    /**
     * Restore pipeline state from a previously serialized state map.
     *
     * @param state map of view name to serialized JSON state
     */
    public void restoreState(Map<String, String> state) {
        for (Map.Entry<String, String> entry : state.entrySet()) {
            String viewName = entry.getKey();
            String pipelineId = viewPipelines.get(viewName);
            if (pipelineId != null) {
                runner.restore(pipelineId, entry.getValue());
            }
        }
    }

    /**
     * Destroy all deployed pipelines and reset state.
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
    }
}

package dev.rtbot.sql;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Local pipeline runner backed by native RTBot pipeline execution via JNI.
 *
 * <p>Port of Python's {@code LocalPipelineRunner}. Each deployed pipeline
 * corresponds to a native handle managed by {@link RtBotSqlCompiler}.
 *
 * <p><b>Thread safety:</b> Not thread-safe. Callers must synchronize externally.
 */
public class LocalPipelineRunner implements PipelineRunner {

    private final Map<String, Long> pipelines = new HashMap<>();
    private int counter = 0;
    private final Gson gson = new Gson();

    // Feed-path profiling counters (single-threaded runtime, no atomics needed).
    private long feedCalls = 0;
    private long inputValues = 0;
    private long marshalNs = 0;
    private long nativeNs = 0;
    private long parseNs = 0;
    private long outputMessages = 0;
    private long outputValues = 0;
    private boolean perfStatsEnabled = false;

    private static final Type OUTPUT_MESSAGE_LIST_TYPE =
            new TypeToken<List<OutputMessage>>() {}.getType();

    private String nextId() {
        return "pipeline_" + (++counter);
    }

    @Override
    public String deploy(String programJson, List<String> sourceStreams, Map<String, String> outputConfig) {
        String id = nextId();
        long handle = RtBotSqlCompiler.createPipeline(programJson);
        pipelines.put(id, handle);
        return id;
    }

    @Override
    public List<OutputMessage> feed(String pipelineId, long timestamp, List<Double> values, String port) {
        Long handle = pipelines.get(pipelineId);
        if (handle == null) {
            throw new IllegalArgumentException("Unknown pipeline: " + pipelineId);
        }
        if (!perfStatsEnabled) {
            if (values.size() == 3) {
                double v0 = values.get(0);
                double v1 = values.get(1);
                double v2 = values.get(2);
                String json = RtBotSqlCompiler.feedPipeline3(handle, timestamp, v0, v1, v2, port);
                return parseOutputMessages(json);
            }
            double[] valArr = new double[values.size()];
            for (int i = 0; i < values.size(); i++) {
                valArr[i] = values.get(i);
            }
            String json = RtBotSqlCompiler.feedPipeline(handle, timestamp, valArr, port);
            return parseOutputMessages(json);
        }

        long t0 = System.nanoTime();
        String json;
        if (values.size() == 3) {
            // Hot path used by vibration_raw ingest loop.
            double v0 = values.get(0);
            double v1 = values.get(1);
            double v2 = values.get(2);
            long t1 = System.nanoTime();
            json = RtBotSqlCompiler.feedPipeline3(handle, timestamp, v0, v1, v2, port);
            long t2 = System.nanoTime();
            List<OutputMessage> outputs = parseOutputMessages(json);
            long t3 = System.nanoTime();

            feedCalls++;
            inputValues += values.size();
            marshalNs += (t1 - t0);
            nativeNs += (t2 - t1);
            parseNs += (t3 - t2);
            outputMessages += outputs.size();
            for (OutputMessage out : outputs) {
                outputValues += out.values != null ? out.values.size() : 0;
            }
            return outputs;
        }

        double[] valArr = new double[values.size()];
        for (int i = 0; i < values.size(); i++) {
            valArr[i] = values.get(i);
        }
        long t1 = System.nanoTime();
        json = RtBotSqlCompiler.feedPipeline(handle, timestamp, valArr, port);
        long t2 = System.nanoTime();
        List<OutputMessage> outputs = parseOutputMessages(json);
        long t3 = System.nanoTime();

        feedCalls++;
        inputValues += values.size();
        marshalNs += (t1 - t0);
        nativeNs += (t2 - t1);
        parseNs += (t3 - t2);
        outputMessages += outputs.size();
        for (OutputMessage out : outputs) {
            outputValues += out.values != null ? out.values.size() : 0;
        }
        return outputs;
    }

    /**
     * Primitive hot path for 3-value messages to avoid per-call boxing/list allocations.
     */
    public List<OutputMessage> feed3(String pipelineId, long timestamp,
                                     double v0, double v1, double v2,
                                     String port) {
        Long handle = pipelines.get(pipelineId);
        if (handle == null) {
            throw new IllegalArgumentException("Unknown pipeline: " + pipelineId);
        }
        if (!perfStatsEnabled) {
            String json;
            if ("i1".equals(port)) {
                json = RtBotSqlCompiler.feedPipeline3I1(handle, timestamp, v0, v1, v2);
            } else {
                json = RtBotSqlCompiler.feedPipeline3(handle, timestamp, v0, v1, v2, port);
            }
            return parseOutputMessages(json);
        }

        long t0 = System.nanoTime();
        String json;
        if ("i1".equals(port)) {
            json = RtBotSqlCompiler.feedPipeline3I1(handle, timestamp, v0, v1, v2);
        } else {
            json = RtBotSqlCompiler.feedPipeline3(handle, timestamp, v0, v1, v2, port);
        }
        long t1 = System.nanoTime();
        List<OutputMessage> outputs = parseOutputMessages(json);
        long t2 = System.nanoTime();

        feedCalls++;
        inputValues += 3;
        nativeNs += (t1 - t0);
        parseNs += (t2 - t1);
        outputMessages += outputs.size();
        for (OutputMessage out : outputs) {
            outputValues += out.values != null ? out.values.size() : 0;
        }
        return outputs;
    }

    /**
     * Batched hot path for 3-value messages on default input port "i1".
     *
     * <p>Feeds all rows in one JNI/native call to amortize boundary overhead.
     */
    public List<OutputMessage> feedBatch3I1(
            String pipelineId,
            long[] timestamps,
            double[] v0,
            double[] v1,
            double[] v2) {
        Long handle = pipelines.get(pipelineId);
        if (handle == null) {
            throw new IllegalArgumentException("Unknown pipeline: " + pipelineId);
        }
        if (timestamps == null || v0 == null || v1 == null || v2 == null) {
            throw new IllegalArgumentException("feedBatch3I1 arrays must not be null");
        }
        int n = timestamps.length;
        if (v0.length != n || v1.length != n || v2.length != n) {
            throw new IllegalArgumentException(
                    "feedBatch3I1 array length mismatch: ts=" + n
                            + ", v0=" + v0.length
                            + ", v1=" + v1.length
                            + ", v2=" + v2.length);
        }

        if (!perfStatsEnabled) {
            String json = RtBotSqlCompiler.feedPipeline3BatchI1(
                    handle, timestamps, v0, v1, v2);
            return parseOutputMessages(json);
        }

        long t0 = System.nanoTime();
        String json = RtBotSqlCompiler.feedPipeline3BatchI1(
                handle, timestamps, v0, v1, v2);
        long t1 = System.nanoTime();
        List<OutputMessage> outputs = parseOutputMessages(json);
        long t2 = System.nanoTime();

        feedCalls++;
        inputValues += (long) n * 3L;
        nativeNs += (t1 - t0);
        parseNs += (t2 - t1);
        outputMessages += outputs.size();
        for (OutputMessage out : outputs) {
            outputValues += out.values != null ? out.values.size() : 0;
        }
        return outputs;
    }

    @Override
    public List<OutputMessage> runOnce(String programJson, List<InputMessage> inputs) {
        long handle = RtBotSqlCompiler.createPipeline(programJson);
        try {
            List<OutputMessage> all = new ArrayList<>();
            for (InputMessage msg : inputs) {
                double[] valArr = new double[msg.values.size()];
                for (int i = 0; i < msg.values.size(); i++) {
                    valArr[i] = msg.values.get(i);
                }
                String json = RtBotSqlCompiler.feedPipeline(handle, msg.timestamp, valArr, msg.port);
                all.addAll(parseOutputMessages(json));
            }
            return all;
        } finally {
            RtBotSqlCompiler.destroyPipeline(handle);
        }
    }

    @Override
    public void destroy(String pipelineId) {
        Long handle = pipelines.remove(pipelineId);
        if (handle != null) {
            RtBotSqlCompiler.destroyPipeline(handle);
        }
    }

    @Override
    public String serialize(String pipelineId) {
        Long handle = pipelines.get(pipelineId);
        if (handle == null) {
            throw new IllegalArgumentException("Unknown pipeline: " + pipelineId);
        }
        return RtBotSqlCompiler.serializePipeline(handle);
    }

    @Override
    public void restore(String pipelineId, String stateJson) {
        Long handle = pipelines.get(pipelineId);
        if (handle == null) {
            throw new IllegalArgumentException("Unknown pipeline: " + pipelineId);
        }
        RtBotSqlCompiler.restorePipeline(handle, stateJson);
    }

    /**
     * Parse the JSON output from the native pipeline feed into a list of {@link OutputMessage}.
     *
     * <p>Expected format: {@code [{"timestamp":..., "values":[...], "operator_id":"...", "port":"..."}]}
     */
    private List<OutputMessage> parseOutputMessages(String json) {
        if (json == null || json.isEmpty() || "[]".equals(json.trim())) {
            return java.util.Collections.emptyList();
        }
        List<OutputMessage> messages = gson.fromJson(json, OUTPUT_MESSAGE_LIST_TYPE);
        return messages != null ? messages : java.util.Collections.emptyList();
    }

    public void resetPerfStats() {
        feedCalls = 0;
        inputValues = 0;
        marshalNs = 0;
        nativeNs = 0;
        parseNs = 0;
        outputMessages = 0;
        outputValues = 0;
    }

    public void setPerfStatsEnabled(boolean enabled) {
        perfStatsEnabled = enabled;
    }

    public Map<String, Long> snapshotPerfStats() {
        Map<String, Long> stats = new LinkedHashMap<>();
        stats.put("runner_feed_calls", feedCalls);
        stats.put("runner_input_values", inputValues);
        stats.put("runner_marshal_ns", marshalNs);
        stats.put("runner_native_call_ns", nativeNs);
        stats.put("runner_json_parse_ns", parseNs);
        stats.put("runner_output_messages", outputMessages);
        stats.put("runner_output_values", outputValues);
        return stats;
    }
}

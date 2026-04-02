package dev.rtbot.sql;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
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
        double[] valArr = new double[values.size()];
        for (int i = 0; i < values.size(); i++) {
            valArr[i] = values.get(i);
        }
        String json = RtBotSqlCompiler.feedPipeline(handle, timestamp, valArr, port);
        return parseOutputMessages(json);
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
            return new ArrayList<>();
        }
        List<OutputMessage> messages = gson.fromJson(json, OUTPUT_MESSAGE_LIST_TYPE);
        return messages != null ? messages : new ArrayList<>();
    }
}

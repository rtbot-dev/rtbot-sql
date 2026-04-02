package dev.rtbot.sql;

import java.util.List;
import java.util.Map;

/**
 * Interface for deploying and running RTBot pipelines.
 *
 * <p>A pipeline is a compiled RTBot program that processes streaming messages.
 * Implementations may run locally (via JNI) or remotely.
 */
public interface PipelineRunner {

    /**
     * Deploy a pipeline from a compiled program JSON.
     *
     * @param programJson   the RTBot program JSON
     * @param sourceStreams  list of source stream names
     * @param outputConfig  output configuration map
     * @return a unique pipeline identifier
     */
    String deploy(String programJson, List<String> sourceStreams, Map<String, String> outputConfig);

    /**
     * Feed a single timestamped message into a deployed pipeline.
     *
     * @param pipelineId the pipeline identifier from {@link #deploy}
     * @param timestamp  monotonically increasing timestamp
     * @param values     value vector
     * @param port       input port (e.g. "i1")
     * @return list of output messages produced
     */
    List<OutputMessage> feed(String pipelineId, long timestamp, List<Double> values, String port);

    /**
     * Run a pipeline ephemerally: create, feed all inputs, collect outputs, destroy.
     *
     * @param programJson the RTBot program JSON
     * @param inputs      list of input messages to feed
     * @return all output messages produced
     */
    List<OutputMessage> runOnce(String programJson, List<InputMessage> inputs);

    /**
     * Destroy a deployed pipeline and free its resources.
     *
     * @param pipelineId the pipeline identifier
     */
    void destroy(String pipelineId);

    /**
     * Serialize a deployed pipeline's state to JSON.
     *
     * @param pipelineId the pipeline identifier
     * @return JSON string of the serialized state
     */
    String serialize(String pipelineId);

    /**
     * Restore a deployed pipeline from a previously serialized state.
     *
     * @param pipelineId the pipeline identifier
     * @param stateJson  JSON state from {@link #serialize}
     */
    void restore(String pipelineId, String stateJson);
}

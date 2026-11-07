package dev.rtbot.sql;

import java.util.ArrayList;
import java.util.List;

/**
 * Callback interface for receiving output messages from streams and views.
 *
 * <p>Inspired by the rtbot-redis subscription pattern: output data is forwarded
 * to active subscribers rather than accumulated in memory. When no listener is
 * registered for a given stream/view, output is discarded (unless the stream
 * is needed for internal propagation or SELECT queries).
 *
 * <p>Implementations might write to an external system (Redis stream, Kafka
 * topic, file), accumulate in a bounded buffer, or simply log for debugging.
 *
 * <p><b>Thread safety:</b> Implementations must handle their own thread safety
 * if the runtime is called from multiple threads (though the runtime itself is
 * not thread-safe).
 *
 * @see RtBotSqlRuntime#subscribe(String, OutputListener)
 * @see RtBotSqlRuntime#unsubscribe(String)
 */
@FunctionalInterface
public interface OutputListener {

    /**
     * Called when a new message is produced for a subscribed stream or view.
     *
     * @param streamName the name of the stream or view that produced the message
     * @param timestamp  the message timestamp (monotonically increasing per stream)
     * @param values     the value vector
     */
    void onMessage(String streamName, long timestamp, List<Double> values);

    /**
     * Primitive fast-path overload. The {@code values} array is a reusable
     * scratch buffer; only indices {@code [0, nValues)} are valid. Callers
     * that need to retain the slice must copy it.
     *
     * <p>The default implementation boxes the primitive slice into a
     * {@link List} and delegates to {@link #onMessage(String, long, List)},
     * preserving backward compatibility for existing lambda subscribers.
     * Override this method to avoid boxing on the hot path.
     *
     * @param streamName the name of the stream or view that produced the message
     * @param timestamp  the message timestamp
     * @param values     reusable scratch array; valid range {@code [0, nValues)}
     * @param nValues    number of valid entries in {@code values}
     */
    default void onMessage(String streamName, long timestamp, double[] values, int nValues) {
        List<Double> boxed = new ArrayList<>(nValues);
        for (int i = 0; i < nValues; i++) boxed.add(values[i]);
        onMessage(streamName, timestamp, boxed);
    }

    /**
     * Wraps an {@link ArrayOutputListener} as an {@link OutputListener} that
     * routes the primitive fast path directly without boxing.
     *
     * <p>The resulting listener's {@link #onMessage(String, long, List)} method
     * boxes values into a primitive array and delegates to the wrapped listener,
     * so backward-compat callers still work.
     *
     * @param delegate the primitive-array listener to wrap
     * @return an {@link OutputListener} that dispatches via the primitive path
     */
    static OutputListener ofArray(ArrayOutputListener delegate) {
        return new OutputListener() {
            @Override
            public void onMessage(String streamName, long timestamp, List<Double> values) {
                double[] arr = new double[values.size()];
                for (int i = 0; i < arr.length; i++) arr[i] = values.get(i);
                delegate.onMessage(streamName, timestamp, arr, arr.length);
            }

            @Override
            public void onMessage(String streamName, long timestamp, double[] values, int nValues) {
                delegate.onMessage(streamName, timestamp, values, nValues);
            }
        };
    }
}

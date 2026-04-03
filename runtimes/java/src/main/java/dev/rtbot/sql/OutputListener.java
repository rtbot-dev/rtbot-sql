package dev.rtbot.sql;

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
}

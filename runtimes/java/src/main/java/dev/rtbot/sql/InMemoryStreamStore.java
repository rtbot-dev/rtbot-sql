package dev.rtbot.sql;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * In-memory message store for streams and materialized views.
 *
 * <p>Port of Python's {@code InMemoryStreamStore}. Stores timestamped message
 * vectors per stream name, supporting append, read, and keyed lookups.
 *
 * <p><b>Thread safety:</b> Not thread-safe. Callers must synchronize externally.
 */
public class InMemoryStreamStore {

    private final Map<String, List<Message>> streams = new LinkedHashMap<>();

    /**
     * Append a message to a stream.
     *
     * @param streamName name of the stream
     * @param timestamp  monotonically increasing timestamp
     * @param values     value vector
     */
    public void append(String streamName, long timestamp, List<Double> values) {
        List<Message> messages = streams.computeIfAbsent(streamName, k -> new ArrayList<>());
        messages.add(new Message(timestamp, new ArrayList<>(values)));
    }

    /**
     * Read all messages from a stream.
     *
     * @return a copy of all messages, or an empty list if stream not found
     */
    public List<Message> read(String streamName) {
        List<Message> data = streams.get(streamName);
        if (data == null) return new ArrayList<>();
        return new ArrayList<>(data);
    }

    /**
     * Read the last N messages from a stream.
     *
     * @param count maximum number of messages to return
     * @return the last {@code count} messages (or fewer if not enough data)
     */
    public List<Message> readLatest(String streamName, int count) {
        if (count <= 0) return new ArrayList<>();
        List<Message> data = streams.get(streamName);
        if (data == null) return new ArrayList<>();
        if (count >= data.size()) return new ArrayList<>(data);
        return new ArrayList<>(data.subList(data.size() - count, data.size()));
    }

    /**
     * Read the latest message per key from a keyed view.
     *
     * <p>Scans backwards through the stream to find the most recent message
     * for each known key value (identified by the value at {@code keyIndex}
     * in the message vector).
     *
     * @param viewName  name of the keyed view stream
     * @param knownKeys set of key values to look for
     * @param keyIndex  index of the key column in the message values
     * @return map from key value to the latest message for that key
     */
    public Map<Double, Message> readLatestPerKey(String viewName, Set<Double> knownKeys, int keyIndex) {
        Map<Double, Message> result = new LinkedHashMap<>();
        if (knownKeys == null || knownKeys.isEmpty()) return result;

        List<Message> data = streams.get(viewName);
        if (data == null) return result;

        for (int i = data.size() - 1; i >= 0; i--) {
            Message msg = data.get(i);
            if (keyIndex < 0 || keyIndex >= msg.values.size()) continue;
            double key = msg.values.get(keyIndex);
            if (!knownKeys.contains(key) || result.containsKey(key)) continue;
            result.put(key, msg);
            if (result.size() == knownKeys.size()) break;
        }

        return result;
    }

    /**
     * Clear all stored messages for a stream.
     */
    public void clear(String streamName) {
        streams.remove(streamName);
    }
}

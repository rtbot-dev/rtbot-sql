package dev.rtbot.sql;

import java.util.ArrayList;
import java.util.List;

/**
 * A timestamped message in a stream.
 */
public class Message {

    public long timestamp;
    public List<Double> values;

    public Message() {
        this.values = new ArrayList<>();
    }

    public Message(long timestamp, List<Double> values) {
        this.timestamp = timestamp;
        this.values = values != null ? new ArrayList<>(values) : new ArrayList<>();
    }

    @Override
    public String toString() {
        return "Message{timestamp=" + timestamp + ", values=" + values + '}';
    }
}

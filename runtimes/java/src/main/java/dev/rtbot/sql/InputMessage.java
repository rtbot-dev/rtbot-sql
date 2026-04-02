package dev.rtbot.sql;

import java.util.ArrayList;
import java.util.List;

/**
 * An input message for pipeline execution.
 */
public class InputMessage {

    public long timestamp;
    public List<Double> values;
    public String port;

    public InputMessage() {
        this.values = new ArrayList<>();
        this.port = "i1";
    }

    public InputMessage(long timestamp, List<Double> values) {
        this.timestamp = timestamp;
        this.values = values != null ? new ArrayList<>(values) : new ArrayList<>();
        this.port = "i1";
    }

    public InputMessage(long timestamp, List<Double> values, String port) {
        this.timestamp = timestamp;
        this.values = values != null ? new ArrayList<>(values) : new ArrayList<>();
        this.port = port != null ? port : "i1";
    }

    @Override
    public String toString() {
        return "InputMessage{timestamp=" + timestamp
                + ", values=" + values
                + ", port='" + port + '\'' + '}';
    }
}

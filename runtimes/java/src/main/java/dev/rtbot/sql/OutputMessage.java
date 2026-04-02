package dev.rtbot.sql;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

/**
 * An output message from a pipeline execution, including the originating operator and port.
 */
public class OutputMessage {

    @SerializedName("timestamp")
    public long timestamp;

    @SerializedName("values")
    public List<Double> values;

    @SerializedName("operator_id")
    public String operatorId;

    @SerializedName("port")
    public String port;

    public OutputMessage() {
        this.values = new ArrayList<>();
        this.operatorId = "";
        this.port = "";
    }

    public OutputMessage(long timestamp, List<Double> values, String operatorId, String port) {
        this.timestamp = timestamp;
        this.values = values != null ? new ArrayList<>(values) : new ArrayList<>();
        this.operatorId = operatorId != null ? operatorId : "";
        this.port = port != null ? port : "";
    }

    @Override
    public String toString() {
        return "OutputMessage{timestamp=" + timestamp
                + ", values=" + values
                + ", operatorId='" + operatorId + '\''
                + ", port='" + port + '\'' + '}';
    }
}

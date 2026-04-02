package dev.rtbot.sql;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

/**
 * Schema for a stream: name and ordered column definitions.
 */
public class StreamSchema {

    @SerializedName("name")
    public String name;

    @SerializedName("columns")
    public List<ColumnDef> columns;

    public StreamSchema() {
        this.columns = new ArrayList<>();
    }

    public StreamSchema(String name, List<ColumnDef> columns) {
        this.name = name;
        this.columns = columns != null ? new ArrayList<>(columns) : new ArrayList<>();
    }

    @Override
    public String toString() {
        return "StreamSchema{name='" + name + "', columns=" + columns + '}';
    }
}

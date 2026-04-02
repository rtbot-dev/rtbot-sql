package dev.rtbot.sql;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

/**
 * Schema for a table: name, columns, changelog stream, and key columns.
 */
public class TableSchema {

    @SerializedName("name")
    public String name;

    @SerializedName("columns")
    public List<ColumnDef> columns;

    @SerializedName("changelog_stream")
    public String changelogStream;

    @SerializedName("key_columns")
    public List<Integer> keyColumns;

    public TableSchema() {
        this.columns = new ArrayList<>();
        this.changelogStream = "";
        this.keyColumns = new ArrayList<>();
    }

    public TableSchema(String name, List<ColumnDef> columns, String changelogStream, List<Integer> keyColumns) {
        this.name = name;
        this.columns = columns != null ? new ArrayList<>(columns) : new ArrayList<>();
        this.changelogStream = changelogStream != null ? changelogStream : "";
        this.keyColumns = keyColumns != null ? new ArrayList<>(keyColumns) : new ArrayList<>();
    }

    @Override
    public String toString() {
        return "TableSchema{name='" + name + "', columns=" + columns + '}';
    }
}

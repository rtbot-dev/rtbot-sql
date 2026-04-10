package dev.rtbot.sql;

import com.google.gson.annotations.SerializedName;

/**
 * A column definition within a stream or table schema.
 */
public class ColumnDef {

    @SerializedName("name")
    public String name;

    @SerializedName("index")
    public int index;

    @SerializedName("type")
    public String type;

    public ColumnDef() {
        this.type = "DOUBLE";
    }

    public ColumnDef(String name, int index) {
        this.name = name;
        this.index = index;
        this.type = "DOUBLE";
    }

    public ColumnDef(String name, int index, String type) {
        this.name = name;
        this.index = index;
        this.type = type != null ? type : "DOUBLE";
    }

    /**
     * Returns {@code true} if this column holds text values.
     */
    public boolean isText() {
        return "TEXT".equals(type);
    }

    @Override
    public String toString() {
        return "ColumnDef{name='" + name + "', index=" + index + ", type='" + type + "'}";
    }
}

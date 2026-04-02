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

    public ColumnDef() {}

    public ColumnDef(String name, int index) {
        this.name = name;
        this.index = index;
    }

    @Override
    public String toString() {
        return "ColumnDef{name='" + name + "', index=" + index + '}';
    }
}

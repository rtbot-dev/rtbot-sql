package dev.rtbot.sql;

import com.google.gson.annotations.SerializedName;

/**
 * Source stream + column for a projected output field. Populated for
 * direct `column AS alias` SELECT items; absent for aggregate /
 * expression outputs. Output decoders use this to find the source TEXT
 * column's {@link StringDictionary} at output-path-build time so a
 * renamed column resolves back to the original string instead of the
 * raw dictionary ID.
 */
public class FieldOrigin {

    @SerializedName("source_stream")
    public String sourceStream;

    @SerializedName("source_column")
    public String sourceColumn;

    public FieldOrigin() {
    }

    public FieldOrigin(String sourceStream, String sourceColumn) {
        this.sourceStream = sourceStream;
        this.sourceColumn = sourceColumn;
    }
}

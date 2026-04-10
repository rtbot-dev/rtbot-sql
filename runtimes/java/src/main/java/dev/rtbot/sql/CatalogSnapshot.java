package dev.rtbot.sql;

import com.google.gson.annotations.SerializedName;

import java.util.HashMap;
import java.util.Map;

/**
 * Snapshot of the entire catalog state, serializable to JSON for the C++ compiler.
 *
 * <p>The JSON representation uses snake_case field names matching the C++ struct layout:
 * <pre>{
 *   "streams": { ... },
 *   "views": { ... },
 *   "tables": { ... }
 * }</pre>
 */
public class CatalogSnapshot {

    @SerializedName("streams")
    public Map<String, StreamSchema> streams;

    @SerializedName("views")
    public Map<String, ViewMeta> views;

    @SerializedName("tables")
    public Map<String, TableSchema> tables;

    @SerializedName("dictionaries")
    public Map<String, Map<Double, String>> dictionaries;

    public CatalogSnapshot() {
        this.streams = new HashMap<>();
        this.views = new HashMap<>();
        this.tables = new HashMap<>();
        this.dictionaries = new HashMap<>();
    }

    @Override
    public String toString() {
        return "CatalogSnapshot{streams=" + streams.size()
                + ", views=" + views.size()
                + ", tables=" + tables.size()
                + ", dictionaries=" + dictionaries.size() + '}';
    }
}

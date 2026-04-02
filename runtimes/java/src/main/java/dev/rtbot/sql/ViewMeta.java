package dev.rtbot.sql;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata for a view (VIEW or MATERIALIZED VIEW) in the catalog.
 */
public class ViewMeta {

    @SerializedName("name")
    public String name;

    @SerializedName("entity_type")
    public String entityType;

    @SerializedName("view_type")
    public String viewType;

    @SerializedName("field_map")
    public Map<String, Integer> fieldMap;

    @SerializedName("source_streams")
    public List<String> sourceStreams;

    @SerializedName("program_json")
    public String programJson;

    @SerializedName("output_stream")
    public String outputStream;

    @SerializedName("per_key_prefix")
    public String perKeyPrefix;

    @SerializedName("known_keys")
    public List<Double> knownKeys;

    @SerializedName("key_index")
    public int keyIndex;

    public ViewMeta() {
        this.fieldMap = new HashMap<>();
        this.sourceStreams = new ArrayList<>();
        this.programJson = "";
        this.outputStream = "";
        this.perKeyPrefix = "";
        this.knownKeys = new ArrayList<>();
        this.keyIndex = -1;
    }

    /**
     * Parse entityType string to {@link EntityType} enum.
     */
    public EntityType entityTypeEnum() {
        if (entityType == null) return null;
        try {
            return EntityType.valueOf(entityType);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Parse viewType string to {@link ViewType} enum.
     */
    public ViewType viewTypeEnum() {
        if (viewType == null) return null;
        try {
            return ViewType.valueOf(viewType);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "ViewMeta{name='" + name + "', entityType='" + entityType
                + "', viewType='" + viewType + "', keyIndex=" + keyIndex + '}';
    }
}

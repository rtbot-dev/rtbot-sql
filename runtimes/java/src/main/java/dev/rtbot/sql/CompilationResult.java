package dev.rtbot.sql;

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Map;

/**
 * Result of compiling a SQL statement via the native RTBot SQL compiler.
 *
 * <p>Fields are populated based on the statement type. For example,
 * {@code programJson} is only set for CREATE VIEW / CREATE MATERIALIZED VIEW / tier-3 SELECT,
 * while {@code insertPayload} is only set for INSERT statements.
 *
 * <p>All field names use {@code @SerializedName} for snake_case JSON deserialization
 * matching the C++ JNI output format.
 */
public class CompilationResult {

    @SerializedName("statement_type")
    public String statementType;

    @SerializedName("entity_name")
    public String entityName;

    @SerializedName("program_json")
    public String programJson;

    @SerializedName("field_map")
    public Map<String, Integer> fieldMap;

    @SerializedName("source_streams")
    public List<String> sourceStreams;

    @SerializedName("view_type")
    public String viewType;

    @SerializedName("key_index")
    public int keyIndex;

    @SerializedName("select_tier")
    public String selectTier;

    @SerializedName("select_limit")
    public int selectLimit;

    @SerializedName("insert_payload")
    public List<Double> insertPayload;

    @SerializedName("stream_schema")
    public StreamSchema streamSchema;

    @SerializedName("table_schema")
    public TableSchema tableSchema;

    @SerializedName("drop_entity_name")
    public String dropEntityName;

    @SerializedName("drop_entity_type")
    public String dropEntityType;

    @SerializedName("errors")
    public List<CompilationError> errors;

    @SerializedName("delete_payload")
    public List<Double> deletePayload;

    @SerializedName("dictionary_updates")
    public Map<String, Map<String, String>> dictionaryUpdates;

    /**
     * Returns {@code true} if the compilation produced errors.
     */
    public boolean hasErrors() {
        return errors != null && !errors.isEmpty();
    }

    /**
     * Parse the {@code statementType} string into a {@link StatementType} enum.
     */
    public StatementType statementTypeEnum() {
        if (statementType == null) return null;
        try {
            return StatementType.valueOf(statementType);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Parse the {@code viewType} string into a {@link ViewType} enum.
     */
    public ViewType viewTypeEnum() {
        if (viewType == null) return null;
        try {
            return ViewType.valueOf(viewType);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Parse the {@code selectTier} string into a {@link SelectTier} enum.
     */
    public SelectTier selectTierEnum() {
        if (selectTier == null) return null;
        try {
            return SelectTier.valueOf(selectTier);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Parse the {@code dropEntityType} string into an {@link EntityType} enum.
     */
    public EntityType dropEntityTypeEnum() {
        if (dropEntityType == null) return null;
        try {
            return EntityType.valueOf(dropEntityType);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Parse the entity type of the created entity based on statement type.
     */
    public EntityType entityTypeEnum() {
        StatementType st = statementTypeEnum();
        if (st == null) return null;
        switch (st) {
            case SELECT_STREAM: return EntityType.STREAM;
            case CREATE_VIEW: return EntityType.VIEW;
            case CREATE_MATERIALIZED_VIEW: return EntityType.MATERIALIZED_VIEW;
            case CREATE_TABLE: return EntityType.TABLE;
            default: return null;
        }
    }

    @Override
    public String toString() {
        return "CompilationResult{statementType='" + statementType
                + "', entityName='" + entityName
                + "', hasErrors=" + hasErrors() + '}';
    }
}

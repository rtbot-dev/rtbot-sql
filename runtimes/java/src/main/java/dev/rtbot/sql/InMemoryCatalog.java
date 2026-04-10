package dev.rtbot.sql;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * In-memory catalog of streams, views, and tables.
 *
 * <p>Port of Python's {@code InMemoryCatalog}. Maintains the current schema
 * state and produces JSON snapshots for the C++ compiler.
 *
 * <p><b>Thread safety:</b> Not thread-safe. Callers must synchronize externally.
 */
public class InMemoryCatalog {

    private final Map<String, StreamSchema> streams = new LinkedHashMap<>();
    private final Map<String, ViewMeta> views = new LinkedHashMap<>();
    private final Map<String, TableSchema> tables = new LinkedHashMap<>();
    private final Map<String, StringDictionary> dictionaries = new LinkedHashMap<>();

    private final Gson gson = new Gson();

    // -----------------------------------------------------------------
    // Registration
    // -----------------------------------------------------------------

    /**
     * Register a stream schema in the catalog.
     */
    public void registerStream(String name, StreamSchema schema) {
        if (schema.name == null || schema.name.isEmpty()) {
            schema.name = name;
        }
        streams.put(name, schema);
    }

    /**
     * Register view metadata in the catalog.
     */
    public void registerView(String name, ViewMeta meta) {
        if (meta.name == null || meta.name.isEmpty()) {
            meta.name = name;
        }
        views.put(name, meta);
    }

    /**
     * Register a table schema in the catalog.
     */
    public void registerTable(String name, TableSchema schema) {
        if (schema.name == null || schema.name.isEmpty()) {
            schema.name = name;
        }
        tables.put(name, schema);
    }

    // -----------------------------------------------------------------
    // Lookups
    // -----------------------------------------------------------------

    /**
     * Look up a stream schema by name.
     *
     * @return the stream schema, or {@code null} if not found
     */
    public StreamSchema lookupStream(String name) {
        return streams.get(name);
    }

    /**
     * Look up view metadata by name.
     *
     * @return the view metadata, or {@code null} if not found
     */
    public ViewMeta lookupView(String name) {
        return views.get(name);
    }

    /**
     * Look up a table schema by name.
     *
     * @return the table schema, or {@code null} if not found
     */
    public TableSchema lookupTable(String name) {
        return tables.get(name);
    }

    // -----------------------------------------------------------------
    // Drop
    // -----------------------------------------------------------------

    /**
     * Remove an entity from all catalog maps.
     *
     * @return {@code true} if anything was removed
     */
    public boolean drop(String name) {
        boolean removed = false;
        if (streams.remove(name) != null) removed = true;
        if (views.remove(name) != null) removed = true;
        if (tables.remove(name) != null) removed = true;
        return removed;
    }

    // -----------------------------------------------------------------
    // Key tracking for KEYED views
    // -----------------------------------------------------------------

    /**
     * Register a key value for a keyed view.
     */
    public void addKey(String viewName, double key) {
        ViewMeta view = views.get(viewName);
        if (view == null) return;
        double keyValue = key;
        if (!view.knownKeys.contains(keyValue)) {
            view.knownKeys.add(keyValue);
        }
    }

    /**
     * Get all known key values for a keyed view.
     */
    public Set<Double> getKnownKeys(String viewName) {
        ViewMeta view = views.get(viewName);
        if (view == null) return Set.of();
        return view.knownKeys.stream().collect(Collectors.toSet());
    }

    // -----------------------------------------------------------------
    // Dictionary storage (TEXT column encoding)
    // -----------------------------------------------------------------

    /**
     * Get or create a {@link StringDictionary} for the given key.
     *
     * <p>The key is typically {@code "stream.column"} for TEXT columns.
     */
    public StringDictionary getOrCreateDictionary(String key) {
        return dictionaries.computeIfAbsent(key, k -> new StringDictionary());
    }

    /**
     * Look up a dictionary by key.
     *
     * @return the dictionary, or {@code null} if not found
     */
    public StringDictionary lookupDictionary(String key) {
        return dictionaries.get(key);
    }

    /**
     * Register an existing dictionary under the given key.
     */
    public void registerDictionary(String key, StringDictionary dict) {
        dictionaries.put(key, dict);
    }

    /**
     * Remove a dictionary by key.
     */
    public void dropDictionary(String key) {
        dictionaries.remove(key);
    }

    /**
     * Returns a copy of all registered dictionaries.
     */
    public Map<String, StringDictionary> getDictionaries() {
        return new LinkedHashMap<>(dictionaries);
    }

    // -----------------------------------------------------------------
    // Snapshot
    // -----------------------------------------------------------------

    /**
     * Build a {@link CatalogSnapshot} from the current catalog state.
     */
    public CatalogSnapshot snapshot() {
        CatalogSnapshot snap = new CatalogSnapshot();

        for (Map.Entry<String, StreamSchema> entry : streams.entrySet()) {
            StreamSchema original = entry.getValue();
            StreamSchema copy = new StreamSchema(original.name, new ArrayList<>(original.columns));
            snap.streams.put(entry.getKey(), copy);
        }

        for (Map.Entry<String, ViewMeta> entry : views.entrySet()) {
            ViewMeta original = entry.getValue();
            ViewMeta copy = new ViewMeta();
            copy.name = original.name;
            copy.entityType = original.entityType;
            copy.viewType = original.viewType;
            copy.fieldMap = new HashMap<>(original.fieldMap);
            copy.sourceStreams = new ArrayList<>(original.sourceStreams);
            copy.programJson = original.programJson;
            copy.outputStream = original.outputStream;
            copy.perKeyPrefix = original.perKeyPrefix;
            copy.knownKeys = new ArrayList<>(original.knownKeys);
            copy.keyIndex = original.keyIndex;
            snap.views.put(entry.getKey(), copy);
        }

        for (Map.Entry<String, TableSchema> entry : tables.entrySet()) {
            TableSchema original = entry.getValue();
            TableSchema copy = new TableSchema(
                    original.name,
                    new ArrayList<>(original.columns),
                    original.changelogStream,
                    original.keyColumns != null ? new ArrayList<>(original.keyColumns) : new ArrayList<>()
            );
            snap.tables.put(entry.getKey(), copy);
        }

        snap.dictionaries = new HashMap<>();
        for (Map.Entry<String, StringDictionary> entry : dictionaries.entrySet()) {
            snap.dictionaries.put(entry.getKey(), entry.getValue().allEntries());
        }

        return snap;
    }

    /**
     * Serialize the current catalog state to JSON matching the C++ {@code catalog_from_json}
     * expected format.
     *
     * <p>The JSON uses snake_case field names because all POJOs are annotated with
     * {@link com.google.gson.annotations.SerializedName}.
     *
     * @return JSON string of the catalog snapshot
     */
    public String snapshotJson() {
        CatalogSnapshot snap = snapshot();
        return gson.toJson(snap);
    }

    // -----------------------------------------------------------------
    // Accessors (for testing / introspection)
    // -----------------------------------------------------------------

    /**
     * Returns a copy of all registered streams.
     */
    public Map<String, StreamSchema> getStreams() {
        return new LinkedHashMap<>(streams);
    }

    /**
     * Returns a copy of all registered views.
     */
    public Map<String, ViewMeta> getViews() {
        return new LinkedHashMap<>(views);
    }

    /**
     * Returns a copy of all registered tables.
     */
    public Map<String, TableSchema> getTables() {
        return new LinkedHashMap<>(tables);
    }
}

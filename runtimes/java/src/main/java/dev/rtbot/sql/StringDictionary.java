package dev.rtbot.sql;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;

public class StringDictionary {

    private final Map<String, Double> strToId = new LinkedHashMap<>();
    private final Map<Double, String> idToStr = new LinkedHashMap<>();
    private double nextId = 1.0;

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() {}.getType();

    public double encode(String value) {
        Double existing = strToId.get(value);
        if (existing != null) return existing;
        double id = nextId++;
        strToId.put(value, id);
        idToStr.put(id, value);
        return id;
    }

    public Double lookup(String value) {
        return strToId.get(value);
    }

    public String decode(double id) {
        return idToStr.get(id);
    }

    public Map<Double, String> allEntries() {
        return new LinkedHashMap<>(idToStr);
    }

    public void restore(Map<Double, String> entries) {
        strToId.clear();
        idToStr.clear();
        nextId = 1.0;
        for (Map.Entry<Double, String> e : entries.entrySet()) {
            strToId.put(e.getValue(), e.getKey());
            idToStr.put(e.getKey(), e.getValue());
            if (e.getKey() >= nextId) {
                nextId = e.getKey() + 1.0;
            }
        }
    }

    /**
     * Directly insert a bidirectional mapping (used for syncing from C++ compiler).
     * If the string or ID already exists, this is a no-op (idempotent).
     */
    public void putMapping(double id, String str) {
        if (strToId.containsKey(str) || idToStr.containsKey(id)) {
            return;  // already mapped
        }
        strToId.put(str, id);
        idToStr.put(id, str);
        if (id >= nextId) {
            nextId = id + 1.0;
        }
    }

    public int size() { return strToId.size(); }
    public boolean isEmpty() { return strToId.isEmpty(); }

    public String toJson() {
        Map<String, String> serialized = new LinkedHashMap<>();
        for (Map.Entry<Double, String> e : idToStr.entrySet()) {
            serialized.put(String.valueOf(e.getKey()), e.getValue());
        }
        return GSON.toJson(serialized);
    }

    public static StringDictionary fromJson(String json) {
        Map<String, String> raw = GSON.fromJson(json, MAP_TYPE);
        StringDictionary dict = new StringDictionary();
        Map<Double, String> entries = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : raw.entrySet()) {
            entries.put(Double.parseDouble(e.getKey()), e.getValue());
        }
        dict.restore(entries);
        return dict;
    }
}

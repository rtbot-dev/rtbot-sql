package dev.rtbot.sql;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Verifies the JNI binding exposes the CREATE STREAM source metadata and
 * the CREATE MATERIALIZED VIEW output_target through compileSqlJson. The
 * gateway module consumes exactly these fields to resolve ignition://
 * bindings, so a regression in rtbot_sql_jni.cpp would otherwise land
 * silently.
 */
public class BindingMetadataTest {

    private static final String EMPTY_CATALOG =
        "{\"streams\":{},\"views\":{},\"tables\":{},\"dictionaries\":{}}";

    private static JsonObject firstResult(String sql, String catalogJson) {
        String json = RtBotSqlCompiler.compileSqlJson(sql, catalogJson, 1_000_000L);
        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
        JsonArray results = obj.getAsJsonArray("results");
        assertTrue("expected at least one result", results.size() > 0);
        return results.get(0).getAsJsonObject();
    }

    @Test
    public void compileSqlJson_surfacesStreamSourceMetadata() {
        JsonObject result = firstResult(
            "CREATE STREAM system_cpu(cpu DOUBLE, x TEXT) FROM 'ignition://{x}/cpu';",
            EMPTY_CATALOG);
        assertEquals(0, result.getAsJsonArray("errors").size());
        JsonObject schema = result.getAsJsonObject("stream_schema");
        assertTrue("missing 'source' key", schema.has("source"));
        JsonObject source = schema.getAsJsonObject("source");
        assertEquals("ignition://{x}/cpu", source.get("name").getAsString());
        assertEquals("scalar", source.get("type").getAsString());
    }

    @Test
    public void compileSqlJson_surfacesOutputTarget() {
        String catalogJson =
            "{\"streams\":{\"trades\":{\"name\":\"trades\",\"columns\":["
            + "{\"name\":\"price\",\"index\":0,\"type\":\"DOUBLE\"},"
            + "{\"name\":\"instrument_id\",\"index\":1,\"type\":\"TEXT\"}]}},"
            + "\"views\":{},\"tables\":{},\"dictionaries\":{}}";
        JsonObject result = firstResult(
            "CREATE MATERIALIZED VIEW stats TO 'ignition://[Coprocessor]proc/stats/{instrument_id}/' "
            + "AS SELECT AVG(price) AS avg, instrument_id "
            + "FROM trades GROUP BY instrument_id;",
            catalogJson);
        assertEquals(0, result.getAsJsonArray("errors").size());
        assertTrue("missing 'output_target' key", result.has("output_target"));
        assertEquals("ignition://[Coprocessor]proc/stats/{instrument_id}/",
                     result.get("output_target").getAsString());
        // Payload = numeric projected columns only; instrument_id (TEXT)
        // is path material.
        JsonArray payload = result.getAsJsonArray("output_payload_columns");
        assertEquals(1, payload.size());
        assertEquals("avg", payload.get(0).getAsString());
    }

    @Test
    public void compileSqlJson_outputTargetAbsentWithoutToClause() {
        String catalogJson =
            "{\"streams\":{\"trades\":{\"name\":\"trades\",\"columns\":["
            + "{\"name\":\"price\",\"index\":0,\"type\":\"DOUBLE\"},"
            + "{\"name\":\"instrument_id\",\"index\":1,\"type\":\"TEXT\"}]}},"
            + "\"views\":{},\"tables\":{},\"dictionaries\":{}}";
        JsonObject result = firstResult(
            "CREATE MATERIALIZED VIEW stats "
            + "AS SELECT AVG(price) AS avg, instrument_id "
            + "FROM trades GROUP BY instrument_id;",
            catalogJson);
        assertEquals(0, result.getAsJsonArray("errors").size());
        assertFalse("output_target must be absent", result.has("output_target"));
    }
}

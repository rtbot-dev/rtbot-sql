package dev.rtbot.sql;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Verifies the JNI binding exposes line/column AND end_line/end_column for
 * every error category. Without these tests, a regression in
 * rtbot_sql_jni.cpp (e.g. removing {"end_line", e.end_line} from the error
 * serializer) would silently land — there is no other check that the
 * Java-level consumer actually receives the new fields.
 */
public class ErrorLocationBindingTest {

    /** Parse the validateSql JSON output and return the first error's JSON object. */
    private static JsonObject firstValidateError(String sql) {
        String json = RtBotSqlCompiler.validateSql(sql);
        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
        assertFalse("expected invalid SQL", obj.get("valid").getAsBoolean());
        JsonArray errs = obj.getAsJsonArray("errors");
        assertTrue("expected at least one error", errs.size() > 0);
        return errs.get(0).getAsJsonObject();
    }

    @Test
    public void validateSql_syntaxErrorIncludesAllSpanFields() {
        JsonObject err = firstValidateError("SELEKT FROM x");
        assertTrue("missing 'line' key",       err.has("line"));
        assertTrue("missing 'column' key",     err.has("column"));
        assertTrue("missing 'end_line' key",   err.has("end_line"));
        assertTrue("missing 'end_column' key", err.has("end_column"));
        assertTrue("end_line >= 1",   err.get("end_line").getAsInt()   >= 1);
        assertTrue("end_column >= 1", err.get("end_column").getAsInt() >= 1);
    }

    @Test
    public void compileSqlJson_semanticErrorIncludesAllSpanFields() {
        // Empty catalog → "unknown source" error from the analyzer.
        String catalogJson = "{\"streams\":{},\"views\":{},\"tables\":{},\"dictionaries\":{}}";
        String json = RtBotSqlCompiler.compileSqlJson(
            "SELECT * FROM nonexistent_stream LIMIT 10",
            catalogJson,
            1_000_000L);
        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
        JsonArray results = obj.getAsJsonArray("results");
        assertTrue(results.size() > 0);
        JsonObject result = results.get(0).getAsJsonObject();
        JsonArray errs = result.getAsJsonArray("errors");
        assertTrue("expected analyzer errors", errs.size() > 0);
        JsonObject err = errs.get(0).getAsJsonObject();
        assertTrue("missing 'end_line' key",   err.has("end_line"));
        assertTrue("missing 'end_column' key", err.has("end_column"));
        assertTrue("end_line >= 1",   err.get("end_line").getAsInt()   >= 1);
        assertTrue("end_column >= 1", err.get("end_column").getAsInt() >= 1);
    }

    @Test
    public void compileSqlJson_converterErrorIncludesAllSpanFields() {
        // BETWEEN is rejected at AST conversion. The new ConverterError
        // path must surface its location through the binding.
        String catalogJson = "{\"streams\":{},\"views\":{},\"tables\":{},\"dictionaries\":{}}";
        String json = RtBotSqlCompiler.compileSqlJson(
            "SELECT 1 FROM x WHERE x BETWEEN 1 AND 10 LIMIT 1",
            catalogJson,
            1_000_000L);
        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
        JsonArray results = obj.getAsJsonArray("results");
        JsonObject result = results.get(0).getAsJsonObject();
        JsonArray errs = result.getAsJsonArray("errors");
        // Find the BETWEEN error.
        JsonObject between = null;
        for (int i = 0; i < errs.size(); i++) {
            JsonObject e = errs.get(i).getAsJsonObject();
            if (e.get("message").getAsString().contains("BETWEEN")) {
                between = e;
                break;
            }
        }
        assertNotNull("expected BETWEEN error", between);
        assertTrue(between.has("end_line"));
        assertTrue(between.has("end_column"));
        assertTrue("end_line >= 1",   between.get("end_line").getAsInt()   >= 1);
        assertTrue("end_column >= 1", between.get("end_column").getAsInt() >= 1);
    }
}

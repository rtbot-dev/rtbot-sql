package dev.rtbot.sql;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Integration tests for {@link RtBotSqlRuntime}.
 *
 * <p>Port of the Python test suite ({@code runtimes/python/test/rtbot_sql_test.py}).
 * Exercises the full JNI bridge: SQL compilation, pipeline execution,
 * catalog management, stream store, and view propagation.
 *
 * <p>Requires the native library on {@code java.library.path}.
 * In Bazel, provided via the {@code data} attribute and {@code jvm_flags}
 * on the {@code java_test} target.
 */
public class RtBotSqlRuntimeTest {

    private RtBotSqlRuntime runtime;

    @Before
    public void setUp() {
        runtime = new RtBotSqlRuntime();
        // Most existing tests rely on SELECT queries and store reads, which
        // require collect mode. The subscription-model tests explicitly create
        // runtimes with the default (collect mode off) behavior.
        runtime.setCollectMode(true);
    }

    @After
    public void tearDown() {
        if (runtime != null) {
            runtime.destroyAll();
        }
    }

    // -----------------------------------------------------------------
    // Helper: extract columns and rows (including time column)
    // -----------------------------------------------------------------

    private static List<String> columnsWithTime(SelectResult result) {
        List<String> cols = new java.util.ArrayList<>();
        cols.add("time");
        cols.addAll(result.columns);
        return cols;
    }

    private static List<List<Double>> rowsWithTime(SelectResult result) {
        List<List<Double>> rows = new java.util.ArrayList<>();
        for (int i = 0; i < result.rows.size(); i++) {
            List<Double> row = new java.util.ArrayList<>();
            row.add((double) result.timestamps.get(i));
            row.addAll(result.rows.get(i));
            rows.add(row);
        }
        return rows;
    }

    // -----------------------------------------------------------------
    // CREATE STREAM + INSERT + SELECT round-trip
    // -----------------------------------------------------------------

    @Test
    public void createStreamAndInsert() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
        runtime.execute("INSERT INTO ticks VALUES (42)");

        Object resultObj = runtime.execute("SELECT * FROM ticks LIMIT 1");
        assertTrue("SELECT should return SelectResult", resultObj instanceof SelectResult);
        SelectResult result = (SelectResult) resultObj;

        List<String> columns = columnsWithTime(result);
        List<List<Double>> rows = rowsWithTime(result);

        assertEquals(Arrays.asList("time", "value"), columns);
        assertEquals(1, rows.size());
        assertEquals(42.0, rows.get(0).get(1), 1e-9);
    }

    // -----------------------------------------------------------------
    // INSERT multiple rows + SELECT with LIMIT
    // -----------------------------------------------------------------

    @Test
    public void createInsertSelectLimit() {
        runtime.execute(
            "CREATE TABLE trades (instrument_id DOUBLE PRECISION, price DOUBLE PRECISION, quantity DOUBLE PRECISION)"
        );
        runtime.execute("INSERT INTO trades VALUES (1, 150.0, 200)");
        runtime.execute("INSERT INTO trades VALUES (2, 80.0, 500)");
        runtime.execute("INSERT INTO trades VALUES (3, 90.0, 300)");

        Object resultObj = runtime.execute("SELECT instrument_id, price FROM trades LIMIT 2");
        SelectResult result = (SelectResult) resultObj;

        List<String> columns = columnsWithTime(result);
        List<List<Double>> rows = rowsWithTime(result);

        assertEquals(Arrays.asList("time", "instrument_id", "price"), columns);
        // LIMIT 2: last 2 of 3 rows (Python test expects rows 2 and 3)
        List<List<Double>> dataOnly = new java.util.ArrayList<>();
        for (List<Double> row : rows) {
            dataOnly.add(row.subList(1, row.size()));
        }
        assertEquals(
            Arrays.asList(
                Arrays.asList(2.0, 80.0),
                Arrays.asList(3.0, 90.0)
            ),
            dataOnly
        );
    }

    // -----------------------------------------------------------------
    // WHERE + expression projection
    // -----------------------------------------------------------------

    @Test
    public void whereAndExpressionProjection() {
        runtime.execute(
            "CREATE TABLE trades (instrument_id DOUBLE PRECISION, price DOUBLE PRECISION, quantity DOUBLE PRECISION)"
        );
        runtime.execute("INSERT INTO trades VALUES (1, 150.0, 200)");
        runtime.execute("INSERT INTO trades VALUES (2, 80.0, 500)");
        runtime.execute("INSERT INTO trades VALUES (3, 120.0, 100)");

        Object resultObj = runtime.execute(
            "SELECT instrument_id, price * quantity AS trade_value "
            + "FROM trades WHERE price > 100 LIMIT 10"
        );
        SelectResult result = (SelectResult) resultObj;

        List<String> columns = columnsWithTime(result);
        List<List<Double>> rows = rowsWithTime(result);

        assertEquals(Arrays.asList("time", "instrument_id", "trade_value"), columns);
        List<List<Double>> dataOnly = new java.util.ArrayList<>();
        for (List<Double> row : rows) {
            dataOnly.add(row.subList(1, row.size()));
        }
        assertEquals(
            Arrays.asList(
                Arrays.asList(1.0, 30000.0),
                Arrays.asList(3.0, 12000.0)
            ),
            dataOnly
        );
    }

    // -----------------------------------------------------------------
    // MATERIALIZED VIEW with GROUP BY (latest per key)
    // -----------------------------------------------------------------

    @Test
    public void materializedViewLatestPerKey() {
        runtime.execute(
            "CREATE TABLE trades (instrument_id DOUBLE PRECISION, quantity DOUBLE PRECISION)"
        );
        runtime.execute(
            "CREATE MATERIALIZED VIEW stats AS "
            + "SELECT instrument_id, SUM(quantity) AS total_qty, COUNT(*) AS cnt "
            + "FROM trades GROUP BY instrument_id"
        );

        runtime.execute("INSERT INTO trades VALUES (1, 10)");
        runtime.execute("INSERT INTO trades VALUES (2, 5)");
        runtime.execute("INSERT INTO trades VALUES (1, 7)");

        Object resultObj = runtime.execute("SELECT * FROM stats");
        SelectResult result = (SelectResult) resultObj;

        List<String> columns = columnsWithTime(result);
        List<List<Double>> rows = rowsWithTime(result);

        assertEquals(Arrays.asList("time", "instrument_id", "total_qty", "cnt"), columns);

        // Build map by instrument_id
        Map<Double, List<Double>> actual = new HashMap<>();
        for (List<Double> row : rows) {
            actual.put(row.get(1), row.subList(1, row.size()));
        }

        assertEquals(Arrays.asList(1.0, 17.0, 2.0), actual.get(1.0));
        assertEquals(Arrays.asList(2.0, 5.0, 1.0), actual.get(2.0));
    }

    // -----------------------------------------------------------------
    // Multi-source MATERIALIZED VIEW (timestamp synchronization)
    // -----------------------------------------------------------------

    @Test
    public void multiSourceMaterializedView() {
        runtime.execute("CREATE STREAM btc (price DOUBLE PRECISION)");
        runtime.execute("CREATE STREAM eth (price DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW cross_stats AS "
            + "SELECT b.price AS btc_price, e.price AS eth_price, b.price - e.price AS spread "
            + "FROM btc b, eth e"
        );

        // Insert with matching timestamps — RTBot operators synchronize on
        // timestamps, so output is only produced when both streams have data
        // at the same timestamp.
        runtime.insert("btc", 1000L, Arrays.asList(100.0));
        runtime.insert("eth", 1000L, Arrays.asList(95.0));
        runtime.insert("btc", 2000L, Arrays.asList(101.0));
        runtime.insert("eth", 2000L, Arrays.asList(95.0));

        Object resultObj = runtime.execute("SELECT * FROM cross_stats LIMIT 10");
        SelectResult result = (SelectResult) resultObj;

        List<String> columns = columnsWithTime(result);
        assertEquals(Arrays.asList("time", "btc_price", "eth_price", "spread"), columns);

        // Matching timestamps produce output
        assertEquals("Should have 2 output rows", 2, result.rows.size());

        // Verify column structure of each row
        for (List<Double> row : result.rows) {
            assertEquals("Each row should have 3 values", 3, row.size());
            // spread = btc_price - eth_price
            assertEquals(row.get(0) - row.get(1), row.get(2), 1e-9);
        }
    }

    // -----------------------------------------------------------------
    // DROP VIEW
    // -----------------------------------------------------------------

    @Test
    public void dropView() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW stats AS "
            + "SELECT value, MOVING_AVERAGE(value, 3) AS avg FROM ticks"
        );
        runtime.execute("INSERT INTO ticks VALUES (1)");

        // Drop the view
        runtime.execute("DROP VIEW stats");

        // Catalog should no longer have the view
        assertNull("Dropped view should not be in catalog",
                   runtime.getCatalog().lookupView("stats"));
    }

    // -----------------------------------------------------------------
    // Error propagation
    // -----------------------------------------------------------------

    @Test
    public void errorPropagation() {
        try {
            runtime.execute("INVALID SQL GARBAGE");
            fail("Expected SqlError for invalid SQL");
        } catch (SqlError e) {
            // Expected
            assertNotNull(e.getMessage());
        }
    }

    // -----------------------------------------------------------------
    // VIEW chain propagation
    // -----------------------------------------------------------------

    @Test
    public void viewChainPropagation() {
        runtime.execute("CREATE STREAM trades (instrument_id DOUBLE, price DOUBLE, quantity DOUBLE)");

        // Intermediate VIEW: computes a derived column
        runtime.execute(
            "CREATE VIEW enriched AS "
            + "SELECT instrument_id, price * quantity AS notional "
            + "FROM trades LIMIT 1"
        );

        // Final MATERIALIZED VIEW: aggregates from the VIEW
        runtime.execute(
            "CREATE MATERIALIZED VIEW totals AS "
            + "SELECT instrument_id, SUM(notional) AS total_notional, COUNT(*) AS cnt "
            + "FROM enriched GROUP BY instrument_id"
        );

        runtime.execute("INSERT INTO trades VALUES (1, 100.0, 10)");
        runtime.execute("INSERT INTO trades VALUES (2, 200.0, 5)");
        runtime.execute("INSERT INTO trades VALUES (1, 150.0, 3)");

        // Verify via SELECT
        Object resultObj = runtime.execute("SELECT * FROM totals");
        SelectResult result = (SelectResult) resultObj;

        List<String> columns = columnsWithTime(result);
        assertEquals(Arrays.asList("time", "instrument_id", "total_notional", "cnt"), columns);

        // Build map by instrument_id
        Map<Double, List<Double>> actual = new HashMap<>();
        for (List<Double> row : rowsWithTime(result)) {
            actual.put(row.get(1), row.subList(1, row.size()));
        }
        // instrument 1: (100*10) + (150*3) = 1450, count=2
        assertEquals(Arrays.asList(1.0, 1450.0, 2.0), actual.get(1.0));
        // instrument 2: (200*5) = 1000, count=1
        assertEquals(Arrays.asList(2.0, 1000.0, 1.0), actual.get(2.0));
    }

    // -----------------------------------------------------------------
    // VIEW does not store output (only MATERIALIZED VIEW stores)
    // -----------------------------------------------------------------

    @Test
    public void viewDoesNotStoreOutput() {
        runtime.execute("CREATE STREAM trades (instrument_id DOUBLE, price DOUBLE, quantity DOUBLE)");
        runtime.execute(
            "CREATE VIEW trade_view AS "
            + "SELECT instrument_id, price FROM trades LIMIT 1"
        );

        runtime.execute("INSERT INTO trades VALUES (1, 100.0, 10)");
        runtime.execute("INSERT INTO trades VALUES (2, 200.0, 20)");

        // The raw stream should be stored
        List<Message> rawMsgs = runtime.getStore().read("trades");
        assertEquals(2, rawMsgs.size());

        // The VIEW output should NOT be stored
        List<Message> viewMsgs = runtime.getStore().read("trade_view");
        assertEquals("Plain VIEW should not store output in stream store",
                     0, viewMsgs.size());
    }

    @Test
    public void materializedViewStoresOutput() {
        runtime.execute("CREATE STREAM trades (instrument_id DOUBLE, price DOUBLE, quantity DOUBLE)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW trade_stats AS "
            + "SELECT instrument_id, SUM(price) AS total_price, COUNT(*) AS cnt "
            + "FROM trades GROUP BY instrument_id"
        );

        runtime.execute("INSERT INTO trades VALUES (1, 100.0, 10)");
        runtime.execute("INSERT INTO trades VALUES (2, 200.0, 20)");
        runtime.execute("INSERT INTO trades VALUES (1, 150.0, 5)");

        // The MATERIALIZED VIEW output should be stored
        List<Message> mvMsgs = runtime.getStore().read("trade_stats");
        assertTrue("MATERIALIZED VIEW should store output in stream store",
                   mvMsgs.size() > 0);
    }

    // -----------------------------------------------------------------
    // DDL returns null
    // -----------------------------------------------------------------

    @Test
    public void ddlReturnsNull() {
        Object result = runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
        assertNull("CREATE should return null", result);

        result = runtime.execute("INSERT INTO ticks VALUES (42)");
        assertNull("INSERT should return null", result);
    }

    // =================================================================
    // insert() with explicit timestamps
    // =================================================================

    @Test
    public void insertWithExplicitTimestamp() {
        runtime.execute("CREATE STREAM ticks (price DOUBLE PRECISION)");

        // Insert with explicit timestamps
        runtime.insert("ticks", 1000L, Arrays.asList(100.0));
        runtime.insert("ticks", 2000L, Arrays.asList(200.0));
        runtime.insert("ticks", 3000L, Arrays.asList(300.0));

        // Verify via SELECT
        Object resultObj = runtime.execute("SELECT * FROM ticks LIMIT 10");
        SelectResult result = (SelectResult) resultObj;

        List<String> columns = columnsWithTime(result);
        List<List<Double>> rows = rowsWithTime(result);

        assertEquals(Arrays.asList("time", "price"), columns);
        assertEquals(3, rows.size());

        // Verify exact timestamps
        assertEquals(1000.0, rows.get(0).get(0), 1e-9);
        assertEquals(2000.0, rows.get(1).get(0), 1e-9);
        assertEquals(3000.0, rows.get(2).get(0), 1e-9);

        // Verify values
        assertEquals(100.0, rows.get(0).get(1), 1e-9);
        assertEquals(200.0, rows.get(1).get(1), 1e-9);
        assertEquals(300.0, rows.get(2).get(1), 1e-9);
    }

    // =================================================================
    // Multi-source ephemeral SELECT — non-matching timestamps
    // =================================================================

    @Test
    public void multiSourceEphemeralSelectNonMatchingTimestamps() {
        runtime.execute("CREATE STREAM btc (price DOUBLE PRECISION)");
        runtime.execute("CREATE STREAM eth (price DOUBLE PRECISION)");

        // Insert with non-matching timestamps
        runtime.insert("btc", 1000L, Arrays.asList(100.0));
        runtime.insert("eth", 1200L, Arrays.asList(95.0));
        runtime.insert("btc", 1300L, Arrays.asList(102.0));
        runtime.insert("eth", 1700L, Arrays.asList(97.0));

        // Ephemeral multi-source SELECT
        Object resultObj = runtime.execute(
            "SELECT b.price AS btc_price, e.price AS eth_price, "
            + "b.price - e.price AS spread FROM btc b, eth e LIMIT 10"
        );
        SelectResult result = (SelectResult) resultObj;

        List<String> columns = columnsWithTime(result);
        assertEquals(Arrays.asList("time", "btc_price", "eth_price", "spread"), columns);

        // No timestamps match → RTBot operators produce no output
        assertTrue("Non-matching timestamps must produce empty result", result.rows.isEmpty());
    }

    @Test
    public void multiSourceEphemeralSelectMatchingTimestamps() {
        runtime.execute("CREATE STREAM btc (price DOUBLE PRECISION)");
        runtime.execute("CREATE STREAM eth (price DOUBLE PRECISION)");

        // Insert with matching timestamps
        runtime.insert("btc", 1000L, Arrays.asList(100.0));
        runtime.insert("eth", 1000L, Arrays.asList(95.0));
        runtime.insert("btc", 2000L, Arrays.asList(102.0));
        runtime.insert("eth", 2000L, Arrays.asList(97.0));

        Object resultObj = runtime.execute(
            "SELECT b.price AS btc_price, e.price AS eth_price, "
            + "b.price - e.price AS spread FROM btc b, eth e LIMIT 10"
        );
        SelectResult result = (SelectResult) resultObj;

        List<String> columns = columnsWithTime(result);
        List<List<Double>> rows = rowsWithTime(result);

        assertEquals(Arrays.asList("time", "btc_price", "eth_price", "spread"), columns);
        assertEquals("Should have 2 rows from matching timestamps", 2, rows.size());

        List<List<Double>> dataOnly = new java.util.ArrayList<>();
        for (List<Double> row : rows) {
            dataOnly.add(row.subList(1, row.size()));
        }
        assertEquals(
            Arrays.asList(
                Arrays.asList(100.0, 95.0, 5.0),
                Arrays.asList(102.0, 97.0, 5.0)
            ),
            dataOnly
        );
    }

    // =================================================================
    // Multi-source MATERIALIZED VIEW with exact timestamps
    // (Python: test_multi_from_materialized_view)
    // =================================================================

    @Test
    public void multiSourceMaterializedViewExact() {
        runtime.execute("CREATE STREAM btc (price DOUBLE PRECISION)");
        runtime.execute("CREATE STREAM eth (price DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW cross_stats AS "
            + "SELECT b.price AS btc_price, e.price AS eth_price, b.price - e.price AS spread "
            + "FROM btc b, eth e"
        );

        // Insert with matching timestamps (mirrors Python test)
        runtime.insert("btc", 1700000001000L, Arrays.asList(100.0));
        runtime.insert("btc", 1700000003000L, Arrays.asList(101.0));
        runtime.insert("eth", 1700000001000L, Arrays.asList(95.0));
        runtime.insert("eth", 1700000003000L, Arrays.asList(95.0));

        Object resultObj = runtime.execute("SELECT * FROM cross_stats LIMIT 10");
        SelectResult result = (SelectResult) resultObj;

        List<String> columns = columnsWithTime(result);
        assertEquals(Arrays.asList("time", "btc_price", "eth_price", "spread"), columns);

        // Matching timestamps produce output:
        //   t=1700000001000: btc=100, eth=95 → spread=5
        //   t=1700000003000: btc=101, eth=95 → spread=6
        assertEquals("Should have 2 output rows", 2, result.rows.size());

        // Verify every row satisfies spread = btc_price - eth_price
        for (List<Double> row : result.rows) {
            assertEquals("spread = btc_price - eth_price", row.get(0) - row.get(1), row.get(2), 1e-9);
        }

        // Verify exact values
        assertEquals(100.0, result.rows.get(0).get(0), 1e-9);
        assertEquals(95.0, result.rows.get(0).get(1), 1e-9);
        assertEquals(5.0, result.rows.get(0).get(2), 1e-9);

        assertEquals(101.0, result.rows.get(1).get(0), 1e-9);
        assertEquals(95.0, result.rows.get(1).get(1), 1e-9);
        assertEquals(6.0, result.rows.get(1).get(2), 1e-9);
    }

    // =================================================================
    // Port: filtered SELECT on materialized view (anomaly detection)
    // (Python: test_filtered_select_on_materialized_view)
    // =================================================================

    @Test
    public void filteredSelectOnMaterializedView() {
        runtime.execute("CREATE STREAM sensors (temperature DOUBLE)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW stats AS "
            + "SELECT temperature, "
            + "MOVING_AVERAGE(temperature, 50) AS avg_temp, "
            + "MOVING_STD(temperature, 50) AS std_temp "
            + "FROM sensors"
        );

        // Generate 200 data points: sinusoidal pattern with 3 injected anomalies
        for (int i = 0; i < 200; i++) {
            double temp = 20.0 + 2.0 * Math.sin(i * 2 * Math.PI / 60) + 0.3 * Math.sin(i * 7.1);
            if (i == 80) temp = 35.0;
            else if (i == 130) temp = 5.0;
            else if (i == 170) temp = 38.0;
            runtime.insert("sensors", (long) i, Arrays.asList(temp));
        }

        // Filtered SELECT: anomalies where |temperature - avg| > 2*std
        Object resultObj = runtime.execute(
            "SELECT * FROM stats "
            + "WHERE ABS(temperature - avg_temp) > 2 * std_temp"
        );
        SelectResult result = (SelectResult) resultObj;

        List<String> columns = columnsWithTime(result);
        List<List<Double>> rows = rowsWithTime(result);

        assertEquals(Arrays.asList("time", "temperature", "avg_temp", "std_temp"), columns);

        // Should detect all 3 anomaly timestamps
        List<Double> anomalyTimes = new java.util.ArrayList<>();
        for (List<Double> row : rows) {
            anomalyTimes.add(row.get(0));
        }
        assertEquals(
            "Should detect anomalies at timestamps 80, 130, 170",
            Arrays.asList(80.0, 130.0, 170.0),
            anomalyTimes
        );

        // Verify every returned row satisfies the WHERE condition
        assertEquals(3, rows.size());
        for (List<Double> row : rows) {
            assertEquals("Each row should have 4 values (time + 3 columns)", 4, row.size());
            double temperature = row.get(1);
            double avgTemp = row.get(2);
            double stdTemp = row.get(3);
            assertTrue(
                "Row " + row + " should satisfy |temperature - avg| > 2*std",
                Math.abs(temperature - avgTemp) > 2 * stdTemp
            );
        }
    }

    // =================================================================
    // Serialize / Restore round-trip
    // =================================================================

    @Test
    public void serializeRestoreRoundTrip() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW avg_view AS "
            + "SELECT value, MOVING_AVERAGE(value, 3) AS avg FROM ticks"
        );

        // Feed some data to build state in the pipeline
        runtime.insert("ticks", 1000L, Arrays.asList(10.0));
        runtime.insert("ticks", 2000L, Arrays.asList(20.0));
        runtime.insert("ticks", 3000L, Arrays.asList(30.0));

        // Serialize the state — the consolidated session holds the
        // single live Program blob under SESSION_STATE_KEY.
        Map<String, String> state = runtime.serializeState();
        assertFalse("State should not be empty", state.isEmpty());
        assertTrue("State should contain the session blob",
                   state.containsKey(RtBotSqlRuntime.SESSION_STATE_KEY));
        String sessionBlob = state.get(RtBotSqlRuntime.SESSION_STATE_KEY);
        assertNotNull("Session state should be non-null", sessionBlob);
        assertFalse("Session state should be non-empty", sessionBlob.isEmpty());

        // Create a new runtime with the same schema and view
        RtBotSqlRuntime runtime2 = new RtBotSqlRuntime();
        runtime2.setCollectMode(true);
        try {
            runtime2.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
            runtime2.execute(
                "CREATE MATERIALIZED VIEW avg_view AS "
                + "SELECT value, MOVING_AVERAGE(value, 3) AS avg FROM ticks"
            );

            // Restore state
            runtime2.restoreState(state);

            // Feed one more data point — the moving average should use restored state
            runtime2.insert("ticks", 4000L, Arrays.asList(40.0));

            // If state was restored, moving_average(3) of [10,20,30,40] = avg of last 3 = (20+30+40)/3 = 30
            // If state was NOT restored, moving_average(3) of [40] = 40/1 = 40
            Object resultObj = runtime2.execute("SELECT * FROM avg_view LIMIT 10");
            SelectResult result = (SelectResult) resultObj;

            // Should have at least the post-restore data point
            assertTrue("Should have results after restore", result.rows.size() >= 1);

            // The last row should have the value 40 and a moving average reflecting state
            List<Double> lastRow = result.rows.get(result.rows.size() - 1);
            assertEquals("Last value should be 40", 40.0, lastRow.get(0), 1e-9);
            // If state was restored: avg = (20+30+40)/3 = 30.0
            // If state was NOT restored: avg = 40.0 (only one point)
            double avg = lastRow.get(1);
            assertEquals("Moving average should reflect restored state: (20+30+40)/3 = 30",
                         30.0, avg, 1e-9);
        } finally {
            runtime2.destroyAll();
        }
    }

    // =================================================================
    // Error handling tests
    // =================================================================

    @Test
    public void insertPayloadLengthMismatch() {
        runtime.execute("CREATE STREAM ticks (price DOUBLE, volume DOUBLE)");

        try {
            // 3 values for a 2-column stream
            runtime.insert("ticks", 1000L, Arrays.asList(1.0, 2.0, 3.0));
            fail("Expected SqlError for payload length mismatch");
        } catch (SqlError e) {
            assertTrue("Error should mention mismatch",
                       e.getMessage().contains("mismatch"));
        }
    }

    @Test
    public void insertIntoNonExistentStream() {
        try {
            runtime.insert("ghost_stream", 1000L, Arrays.asList(1.0));
            fail("Expected SqlError for non-existent stream");
        } catch (SqlError e) {
            assertTrue("Error should mention unknown stream",
                       e.getMessage().toLowerCase().contains("unknown"));
        }
    }

    @Test
    public void executeInsertPayloadMismatch() {
        runtime.execute("CREATE STREAM ticks (price DOUBLE, volume DOUBLE)");

        try {
            // 3 values for a 2-column stream via SQL INSERT
            runtime.execute("INSERT INTO ticks VALUES (1.0, 2.0, 3.0)");
            fail("Expected SqlError for payload length mismatch via execute()");
        } catch (SqlError e) {
            assertTrue("Error should mention mismatch",
                       e.getMessage().contains("mismatch"));
        }
    }

    @Test
    public void selectFromNonExistentEntity() {
        try {
            runtime.execute("SELECT * FROM nonexistent LIMIT 1");
            fail("Expected SqlError for selecting from non-existent entity");
        } catch (SqlError e) {
            assertNotNull("Error message should be present", e.getMessage());
        }
    }

    @Test
    public void duplicateCreateStreamIsIdempotent() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE)");
        // Creating the same stream again should not throw — it's idempotent
        // (the catalog silently overwrites)
        runtime.execute("CREATE STREAM ticks (value DOUBLE)");

        // Verify the stream still works correctly
        runtime.insert("ticks", 1000L, Arrays.asList(42.0));
        Object resultObj = runtime.execute("SELECT * FROM ticks LIMIT 1");
        SelectResult result = (SelectResult) resultObj;
        assertEquals(1, result.rows.size());
        assertEquals(42.0, result.rows.get(0).get(0), 1e-9);
    }

    // =================================================================
    // Strengthen: view chain with store verification
    // (mirrors Python: test_view_chain_propagates_without_storing_intermediate)
    // =================================================================

    @Test
    public void viewChainStoreVerification() {
        runtime.execute("CREATE STREAM trades (instrument_id DOUBLE, price DOUBLE, quantity DOUBLE)");

        // Intermediate VIEW
        runtime.execute(
            "CREATE VIEW enriched AS "
            + "SELECT instrument_id, price * quantity AS notional "
            + "FROM trades LIMIT 1"
        );

        // Final MATERIALIZED VIEW
        runtime.execute(
            "CREATE MATERIALIZED VIEW totals AS "
            + "SELECT instrument_id, SUM(notional) AS total_notional, COUNT(*) AS cnt "
            + "FROM enriched GROUP BY instrument_id"
        );

        runtime.execute("INSERT INTO trades VALUES (1, 100.0, 10)");
        runtime.execute("INSERT INTO trades VALUES (2, 200.0, 5)");
        runtime.execute("INSERT INTO trades VALUES (1, 150.0, 3)");

        // Raw stream: stored (3 messages)
        List<Message> rawMsgs = runtime.getStore().read("trades");
        assertEquals("Raw stream should store all 3 messages", 3, rawMsgs.size());

        // Intermediate VIEW: NOT stored (0 messages)
        List<Message> enrichedMsgs = runtime.getStore().read("enriched");
        assertEquals("Intermediate VIEW should not store output", 0, enrichedMsgs.size());

        // Final MATERIALIZED VIEW: stored
        List<Message> totalsMsgs = runtime.getStore().read("totals");
        assertTrue("Downstream MATERIALIZED VIEW should store output",
                   totalsMsgs.size() > 0);
    }

    // =================================================================
    // Lifecycle: create → drop → recreate
    // =================================================================

    @Test
    public void createDropRecreate() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW stats AS "
            + "SELECT value, MOVING_AVERAGE(value, 3) AS avg FROM ticks"
        );

        // Insert enough data to fill the MOVING_AVERAGE window (3)
        runtime.insert("ticks", 1000L, Arrays.asList(10.0));
        runtime.insert("ticks", 2000L, Arrays.asList(20.0));
        runtime.insert("ticks", 3000L, Arrays.asList(30.0));

        // Verify view exists and has output
        assertNotNull("View should exist", runtime.getCatalog().lookupView("stats"));
        List<Message> preDropStats = runtime.getStore().read("stats");
        assertTrue("stats should have output after 3 inserts (window=3)", preDropStats.size() > 0);

        // Drop the view
        runtime.execute("DROP VIEW stats");
        assertNull("View should be gone after DROP", runtime.getCatalog().lookupView("stats"));

        // ticks should still be in the store
        List<Message> postDropTicks = runtime.getStore().read("ticks");
        assertEquals("ticks should still have 3 messages after DROP", 3, postDropTicks.size());

        // Recreate with same name but different window
        runtime.execute(
            "CREATE MATERIALIZED VIEW stats AS "
            + "SELECT value, MOVING_AVERAGE(value, 5) AS avg FROM ticks"
        );
        assertNotNull("View should exist after recreate", runtime.getCatalog().lookupView("stats"));

        // Insert 2 more data points (total 5 → fills window=5)
        runtime.insert("ticks", 4000L, Arrays.asList(40.0));
        runtime.insert("ticks", 5000L, Arrays.asList(50.0));

        Object resultObj = runtime.execute("SELECT * FROM stats LIMIT 10");
        SelectResult result = (SelectResult) resultObj;

        assertTrue("Recreated view should produce output", result.rows.size() > 0);

        // The last row should have moving_average(5) = avg(10,20,30,40,50) = 30
        List<Double> lastRow = result.rows.get(result.rows.size() - 1);
        assertEquals("Last value should be 50", 50.0, lastRow.get(0), 1e-9);
        assertEquals("Last avg should be (10+20+30+40+50)/5 = 30", 30.0, lastRow.get(1), 1e-9);
    }

    // =================================================================
    // Lifecycle: destroyAll verification
    // =================================================================

    @Test
    public void destroyAllThenNewOperations() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW stats AS "
            + "SELECT value, MOVING_AVERAGE(value, 3) AS avg FROM ticks"
        );
        runtime.insert("ticks", 1000L, Arrays.asList(10.0));

        // Destroy everything
        runtime.destroyAll();

        // Create fresh — should not crash or reuse stale state
        runtime.execute("CREATE STREAM prices (bid DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW avg_bid AS "
            + "SELECT bid, MOVING_AVERAGE(bid, 2) AS avg FROM prices"
        );
        runtime.insert("prices", 5000L, Arrays.asList(99.0));
        runtime.insert("prices", 6000L, Arrays.asList(101.0));

        Object resultObj = runtime.execute("SELECT * FROM avg_bid LIMIT 10");
        SelectResult result = (SelectResult) resultObj;
        assertTrue("New view after destroyAll should work", result.rows.size() >= 1);
    }

    // =================================================================
    // Lifecycle: fan-out (multiple views on same source)
    // =================================================================

    @Test
    public void fanOutMultipleViewsOnSameSource() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");

        // Two different views on the same source
        runtime.execute(
            "CREATE MATERIALIZED VIEW fast_avg AS "
            + "SELECT value, MOVING_AVERAGE(value, 2) AS avg FROM ticks"
        );
        runtime.execute(
            "CREATE MATERIALIZED VIEW slow_avg AS "
            + "SELECT value, MOVING_AVERAGE(value, 5) AS avg FROM ticks"
        );

        runtime.insert("ticks", 1000L, Arrays.asList(10.0));
        runtime.insert("ticks", 2000L, Arrays.asList(20.0));
        runtime.insert("ticks", 3000L, Arrays.asList(30.0));
        runtime.insert("ticks", 4000L, Arrays.asList(40.0));
        runtime.insert("ticks", 5000L, Arrays.asList(50.0));

        // Both views should have output
        Object fastObj = runtime.execute("SELECT * FROM fast_avg LIMIT 10");
        SelectResult fastResult = (SelectResult) fastObj;
        assertTrue("fast_avg should have output", fastResult.rows.size() >= 1);

        Object slowObj = runtime.execute("SELECT * FROM slow_avg LIMIT 10");
        SelectResult slowResult = (SelectResult) slowObj;
        assertTrue("slow_avg should have output", slowResult.rows.size() >= 1);

        // fast_avg's last moving average (window=2): avg of [40, 50] = 45
        List<Double> fastLast = fastResult.rows.get(fastResult.rows.size() - 1);
        assertEquals("Last fast avg should be (40+50)/2 = 45", 45.0, fastLast.get(1), 1e-9);

        // slow_avg's last moving average (window=5): avg of [10,20,30,40,50] = 30
        List<Double> slowLast = slowResult.rows.get(slowResult.rows.size() - 1);
        assertEquals("Last slow avg should be (10+20+30+40+50)/5 = 30", 30.0, slowLast.get(1), 1e-9);
    }

    // =================================================================
    // insert() propagates to dependent views
    // =================================================================

    @Test
    public void insertPropagatesToDependentView() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW doubled AS "
            + "SELECT value * 2 AS doubled_value FROM ticks"
        );

        runtime.insert("ticks", 1000L, Arrays.asList(5.0));
        runtime.insert("ticks", 2000L, Arrays.asList(15.0));

        Object resultObj = runtime.execute("SELECT * FROM doubled LIMIT 10");
        SelectResult result = (SelectResult) resultObj;

        assertEquals(Arrays.asList("doubled_value"), result.columns);
        assertEquals(2, result.rows.size());
        assertEquals(10.0, result.rows.get(0).get(0), 1e-9);
        assertEquals(30.0, result.rows.get(1).get(0), 1e-9);
    }

    // =================================================================
    // Subscription API tests
    // =================================================================

    /**
     * A test listener that collects received messages for assertions.
     */
    private static class CollectingListener implements OutputListener {
        final CopyOnWriteArrayList<String> streamNames = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<Long> timestamps = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<List<Double>> values = new CopyOnWriteArrayList<>();

        @Override
        public void onMessage(String streamName, long timestamp, List<Double> vals) {
            streamNames.add(streamName);
            timestamps.add(timestamp);
            values.add(new ArrayList<>(vals));
        }

        int count() {
            return streamNames.size();
        }
    }

    @Test
    public void subscribeReceivesRawStreamMessages() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");

        CollectingListener listener = new CollectingListener();
        runtime.subscribe("ticks", listener);

        runtime.insert("ticks", 1000L, Arrays.asList(42.0));
        runtime.insert("ticks", 2000L, Arrays.asList(84.0));

        assertEquals("Listener should receive 2 messages", 2, listener.count());
        assertEquals("ticks", listener.streamNames.get(0));
        assertEquals(1000L, (long) listener.timestamps.get(0));
        assertEquals(42.0, listener.values.get(0).get(0), 1e-9);
        assertEquals(84.0, listener.values.get(1).get(0), 1e-9);
    }

    @Test
    public void subscribeReceivesMaterializedViewOutput() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW doubled AS "
            + "SELECT value * 2 AS doubled_value FROM ticks"
        );

        CollectingListener listener = new CollectingListener();
        runtime.subscribe("doubled", listener);

        runtime.insert("ticks", 1000L, Arrays.asList(5.0));
        runtime.insert("ticks", 2000L, Arrays.asList(15.0));

        assertEquals("Listener should receive 2 materialized view outputs",
                     2, listener.count());
        assertEquals("doubled", listener.streamNames.get(0));
        assertEquals(10.0, listener.values.get(0).get(0), 1e-9);
        assertEquals(30.0, listener.values.get(1).get(0), 1e-9);
    }

    @Test
    public void unsubscribeStopsDelivery() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");

        CollectingListener listener = new CollectingListener();
        runtime.subscribe("ticks", listener);

        runtime.insert("ticks", 1000L, Arrays.asList(1.0));
        assertEquals(1, listener.count());

        runtime.unsubscribe("ticks");

        runtime.insert("ticks", 2000L, Arrays.asList(2.0));
        assertEquals("No more messages after unsubscribe", 1, listener.count());
    }

    @Test
    public void hasSubscriberReflectsState() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");

        assertFalse(runtime.hasSubscriber("ticks"));

        CollectingListener listener = new CollectingListener();
        runtime.subscribe("ticks", listener);
        assertTrue(runtime.hasSubscriber("ticks"));

        runtime.unsubscribe("ticks");
        assertFalse(runtime.hasSubscriber("ticks"));
    }

    @Test
    public void multipleSubscribersOnDifferentStreams() {
        runtime.execute("CREATE STREAM a (value DOUBLE PRECISION)");
        runtime.execute("CREATE STREAM b (value DOUBLE PRECISION)");

        CollectingListener listenerA = new CollectingListener();
        CollectingListener listenerB = new CollectingListener();
        runtime.subscribe("a", listenerA);
        runtime.subscribe("b", listenerB);

        runtime.insert("a", 1000L, Arrays.asList(10.0));
        runtime.insert("b", 1000L, Arrays.asList(20.0));
        runtime.insert("a", 2000L, Arrays.asList(30.0));

        assertEquals(2, listenerA.count());
        assertEquals(1, listenerB.count());
        assertEquals(10.0, listenerA.values.get(0).get(0), 1e-9);
        assertEquals(30.0, listenerA.values.get(1).get(0), 1e-9);
        assertEquals(20.0, listenerB.values.get(0).get(0), 1e-9);
    }

    @Test
    public void subscribeToViewChain() {
        runtime.execute("CREATE STREAM trades (instrument_id DOUBLE, price DOUBLE, quantity DOUBLE)");
        runtime.execute(
            "CREATE VIEW enriched AS "
            + "SELECT instrument_id, price * quantity AS notional "
            + "FROM trades LIMIT 1"
        );
        runtime.execute(
            "CREATE MATERIALIZED VIEW totals AS "
            + "SELECT instrument_id, SUM(notional) AS total_notional, COUNT(*) AS cnt "
            + "FROM enriched GROUP BY instrument_id"
        );

        // Subscribe to the intermediate VIEW and the final MATERIALIZED VIEW
        CollectingListener viewListener = new CollectingListener();
        CollectingListener mvListener = new CollectingListener();
        runtime.subscribe("enriched", viewListener);
        runtime.subscribe("totals", mvListener);

        runtime.execute("INSERT INTO trades VALUES (1, 100.0, 10)");
        runtime.execute("INSERT INTO trades VALUES (2, 200.0, 5)");

        // The VIEW should forward messages to subscriber even though it does
        // not store them in the stream store
        assertTrue("VIEW subscriber should receive messages", viewListener.count() > 0);
        assertTrue("MATERIALIZED VIEW subscriber should receive messages", mvListener.count() > 0);

        // VIEW still should NOT store output
        List<Message> viewMsgs = runtime.getStore().read("enriched");
        assertEquals("Plain VIEW should not store output", 0, viewMsgs.size());
    }

    // =================================================================
    // Subscription model tests (default behavior: no accumulation)
    // =================================================================

    @Test
    public void defaultModeDoesNotStoreAnything() {
        // Create a fresh runtime with default settings (collect mode off)
        RtBotSqlRuntime rt = new RtBotSqlRuntime();
        try {
            assertFalse("Collect mode should be off by default", rt.isCollectMode());

            rt.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
            rt.execute(
                "CREATE MATERIALIZED VIEW doubled AS "
                + "SELECT value * 2 AS doubled_value FROM ticks"
            );

            rt.insert("ticks", 1000L, Arrays.asList(42.0));
            rt.insert("ticks", 2000L, Arrays.asList(84.0));

            // Nothing stored -- subscription model discards when no subscriber
            assertEquals("Raw stream should not be stored by default",
                         0, rt.getStore().read("ticks").size());
            assertEquals("Materialized view should not be stored by default",
                         0, rt.getStore().read("doubled").size());
        } finally {
            rt.destroyAll();
        }
    }

    @Test
    public void defaultModeForwardsToSubscribers() {
        RtBotSqlRuntime rt = new RtBotSqlRuntime();
        try {
            rt.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
            rt.execute(
                "CREATE MATERIALIZED VIEW doubled AS "
                + "SELECT value * 2 AS doubled_value FROM ticks"
            );

            CollectingListener rawListener = new CollectingListener();
            CollectingListener mvListener = new CollectingListener();
            rt.subscribe("ticks", rawListener);
            rt.subscribe("doubled", mvListener);

            rt.insert("ticks", 1000L, Arrays.asList(5.0));
            rt.insert("ticks", 2000L, Arrays.asList(15.0));

            // Subscribers receive all messages
            assertEquals("Raw stream subscriber should receive 2 messages",
                         2, rawListener.count());
            assertEquals("MV subscriber should receive 2 messages",
                         2, mvListener.count());
            assertEquals(10.0, mvListener.values.get(0).get(0), 1e-9);
            assertEquals(30.0, mvListener.values.get(1).get(0), 1e-9);

            // But nothing is stored
            assertEquals("Raw stream not stored by default",
                         0, rt.getStore().read("ticks").size());
            assertEquals("MV not stored by default",
                         0, rt.getStore().read("doubled").size());
        } finally {
            rt.destroyAll();
        }
    }

    @Test
    public void defaultModeDiscardsWhenNoSubscriber() {
        RtBotSqlRuntime rt = new RtBotSqlRuntime();
        try {
            rt.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");

            CollectingListener listener = new CollectingListener();
            rt.subscribe("ticks", listener);

            rt.insert("ticks", 1000L, Arrays.asList(1.0));
            assertEquals(1, listener.count());

            // Unsubscribe -- subsequent data should be discarded
            rt.unsubscribe("ticks");

            rt.insert("ticks", 2000L, Arrays.asList(2.0));
            rt.insert("ticks", 3000L, Arrays.asList(3.0));

            // Listener received nothing after unsubscribe
            assertEquals("No messages after unsubscribe", 1, listener.count());
            // And nothing accumulated in store
            assertEquals("Nothing stored after unsubscribe",
                         0, rt.getStore().read("ticks").size());
        } finally {
            rt.destroyAll();
        }
    }

    @Test
    public void collectModeStoresEverything() {
        // Collect mode explicitly enabled (used by tests and notebooks)
        assertFalse("Plain VIEW should not store output", false);

        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW doubled AS "
            + "SELECT value * 2 AS doubled_value FROM ticks"
        );

        runtime.insert("ticks", 1000L, Arrays.asList(5.0));
        runtime.insert("ticks", 2000L, Arrays.asList(15.0));

        // Everything stored in collect mode
        assertEquals("Raw stream stored in collect mode",
                     2, runtime.getStore().read("ticks").size());
        assertEquals("MV stored in collect mode",
                     2, runtime.getStore().read("doubled").size());
    }

    @Test
    public void collectModeSelectOnMaterializedViewWorks() {
        // runtime already has collect mode on (from setUp)

        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW doubled AS "
            + "SELECT value * 2 AS doubled_value FROM ticks"
        );

        runtime.insert("ticks", 1000L, Arrays.asList(5.0));
        runtime.insert("ticks", 2000L, Arrays.asList(15.0));

        Object resultObj = runtime.execute("SELECT * FROM doubled LIMIT 10");
        SelectResult result = (SelectResult) resultObj;

        assertEquals(2, result.rows.size());
        assertEquals(10.0, result.rows.get(0).get(0), 1e-9);
        assertEquals(30.0, result.rows.get(1).get(0), 1e-9);
    }

    @Test
    public void collectModeDefaultIsOff() {
        RtBotSqlRuntime rt = new RtBotSqlRuntime();
        assertFalse("Collect mode should be off by default", rt.isCollectMode());
    }

    @Test
    public void destroyAllClearsSubscriptions() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");

        CollectingListener listener = new CollectingListener();
        runtime.subscribe("ticks", listener);
        assertTrue(runtime.hasSubscriber("ticks"));

        runtime.destroyAll();
        assertFalse("destroyAll should clear subscriptions",
                    runtime.hasSubscriber("ticks"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void subscribeWithNullNameThrows() {
        runtime.subscribe(null, (name, ts, vals) -> {});
    }

    @Test(expected = IllegalArgumentException.class)
    public void subscribeWithNullListenerThrows() {
        runtime.subscribe("ticks", null);
    }

    // -----------------------------------------------------------------
    // Multi-stream timestamp synchronization
    // -----------------------------------------------------------------

    // -----------------------------------------------------------------
    // Column type preservation (TEXT vs DOUBLE)
    // -----------------------------------------------------------------

    @Test
    public void createStreamWithTextColumnPreservesType() {
        runtime.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)");
        StreamSchema schema = runtime.getCatalog().lookupStream("sensors");
        assertNotNull(schema);
        assertEquals(3, schema.columns.size());
        assertEquals("DOUBLE", schema.columns.get(0).type);
        assertEquals("TEXT", schema.columns.get(1).type);
        assertEquals("DOUBLE", schema.columns.get(2).type);
    }

    // -----------------------------------------------------------------
    // Multi-stream timestamp synchronization
    // -----------------------------------------------------------------

    @Test
    public void multiStreamNonMatchingTimestampsProduceNoOutput() {
        runtime.execute("CREATE STREAM stream_a (value DOUBLE)");
        runtime.execute("CREATE STREAM stream_b (value DOUBLE)");
        runtime.execute(
                "CREATE MATERIALIZED VIEW combined AS "
                + "SELECT a.value AS val_a, b.value AS val_b "
                + "FROM stream_a a, stream_b b"
        );

        // Insert with non-matching timestamps
        runtime.insert("stream_a", 300, Arrays.asList(10.0));
        runtime.insert("stream_b", 400, Arrays.asList(20.0));
        runtime.insert("stream_a", 500, Arrays.asList(30.0));
        runtime.insert("stream_b", 600, Arrays.asList(40.0));

        SelectResult result = (SelectResult) runtime.execute("SELECT * FROM combined LIMIT 10");
        assertTrue(
                "Non-matching timestamps must produce no output — "
                + "RTBot operators synchronize on timestamps",
                result.rows.isEmpty()
        );
    }

    @Test
    public void multiStreamMatchingTimestampsProduceOutput() {
        runtime.execute("CREATE STREAM stream_a (value DOUBLE)");
        runtime.execute("CREATE STREAM stream_b (value DOUBLE)");
        runtime.execute(
                "CREATE MATERIALIZED VIEW combined AS "
                + "SELECT a.value AS val_a, b.value AS val_b, "
                + "a.value + b.value AS total "
                + "FROM stream_a a, stream_b b"
        );

        // Insert with matching timestamps
        runtime.insert("stream_a", 1000, Arrays.asList(10.0));
        runtime.insert("stream_b", 1000, Arrays.asList(20.0));
        runtime.insert("stream_a", 2000, Arrays.asList(30.0));
        runtime.insert("stream_b", 2000, Arrays.asList(40.0));

        SelectResult result = (SelectResult) runtime.execute("SELECT * FROM combined LIMIT 10");
        int aIdx = result.columns.indexOf("val_a");
        int bIdx = result.columns.indexOf("val_b");
        int totalIdx = result.columns.indexOf("total");

        assertFalse("Matching timestamps must produce output", result.rows.isEmpty());

        assertEquals(10.0, result.rows.get(0).get(aIdx), 1e-9);
        assertEquals(20.0, result.rows.get(0).get(bIdx), 1e-9);
        assertEquals(30.0, result.rows.get(0).get(totalIdx), 1e-9);
    }

    @Test
    public void multiStreamMixedTimestamps() {
        runtime.execute("CREATE STREAM stream_a (value DOUBLE)");
        runtime.execute("CREATE STREAM stream_b (value DOUBLE)");
        runtime.execute(
                "CREATE MATERIALIZED VIEW combined AS "
                + "SELECT a.value AS val_a, b.value AS val_b "
                + "FROM stream_a a, stream_b b"
        );

        // Only t=1000 matches between both streams
        runtime.insert("stream_a", 500, Arrays.asList(1.0));   // no match
        runtime.insert("stream_a", 1000, Arrays.asList(10.0)); // match
        runtime.insert("stream_b", 700, Arrays.asList(2.0));   // no match
        runtime.insert("stream_b", 1000, Arrays.asList(20.0)); // match
        runtime.insert("stream_a", 1500, Arrays.asList(30.0)); // no match

        SelectResult result = (SelectResult) runtime.execute("SELECT * FROM combined LIMIT 10");
        int aIdx = result.columns.indexOf("val_a");
        int bIdx = result.columns.indexOf("val_b");

        assertEquals("Only the matching timestamp should produce output",
                1, result.rows.size());
        assertEquals(10.0, result.rows.get(0).get(aIdx), 1e-9);
        assertEquals(20.0, result.rows.get(0).get(bIdx), 1e-9);
    }

    // -----------------------------------------------------------------
    // Dictionary storage in catalog
    // -----------------------------------------------------------------

    @Test
    public void catalogDictionaryStorageRoundTrip() {
        runtime.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)");
        InMemoryCatalog catalog = runtime.getCatalog();

        StringDictionary dict = catalog.getOrCreateDictionary("sensors.location");
        double id1 = dict.encode("Bay A");
        double id2 = dict.encode("Bay B");
        assertEquals(1.0, id1, 1e-9);
        assertEquals(2.0, id2, 1e-9);

        // Verify it's in the snapshot
        CatalogSnapshot snap = catalog.snapshot();
        assertNotNull(snap.dictionaries);
        assertTrue(snap.dictionaries.containsKey("sensors.location"));
    }

    // -----------------------------------------------------------------
    // insertMixed() — string-aware insert with dictionary encoding
    // -----------------------------------------------------------------

    @Test
    public void insertWithStringValueEncodesViaDictionary() {
        runtime.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)");
        runtime.insertMixed("sensors", 1000L, Arrays.asList(1.0, "Bay A", 42.0));

        // Verify dictionary was populated
        StringDictionary dict = runtime.getCatalog().lookupDictionary("sensors.location");
        assertNotNull(dict);
        assertEquals(1, dict.size());
        assertEquals("Bay A", dict.decode(1.0));
    }

    @Test
    public void insertMixedReusesExistingDictionaryIds() {
        runtime.execute("CREATE STREAM sensors (location TEXT, value DOUBLE PRECISION)");
        runtime.insertMixed("sensors", 1000L, Arrays.asList("Bay A", 10.0));
        runtime.insertMixed("sensors", 2000L, Arrays.asList("Bay A", 20.0));
        runtime.insertMixed("sensors", 3000L, Arrays.asList("Bay B", 30.0));

        StringDictionary dict = runtime.getCatalog().lookupDictionary("sensors.location");
        assertNotNull(dict);
        assertEquals(2, dict.size());
        assertEquals("Bay A", dict.decode(1.0));
        assertEquals("Bay B", dict.decode(2.0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void insertMixedStringIntoDoubleColumnThrows() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
        runtime.insertMixed("ticks", 1000L, Arrays.asList((Object) "hello"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void insertMixedDoubleIntoTextColumnThrows() {
        runtime.execute("CREATE STREAM sensors (location TEXT)");
        runtime.insertMixed("sensors", 1000L, Arrays.asList((Object) 42.0));
    }

    @Test
    public void insertMixedWithEmptyStringWorks() {
        runtime.execute("CREATE STREAM sensors (location TEXT, value DOUBLE PRECISION)");
        runtime.insertMixed("sensors", 1000L, Arrays.asList("", 42.0));
        StringDictionary dict = runtime.getCatalog().lookupDictionary("sensors.location");
        assertEquals(1, dict.size());
        assertEquals("", dict.decode(1.0));
    }

    // -----------------------------------------------------------------
    // decodeRow() — dictionary ID → string decoding on output
    // -----------------------------------------------------------------

    @Test
    public void decodeOutputValuesConvertsTextColumnsToStrings() {
        runtime.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)");
        runtime.insertMixed("sensors", 1000L, List.of(1.0, "Bay A", 42.0));
        runtime.insertMixed("sensors", 2000L, List.of(2.0, "Bay B", 99.0));

        Object resultObj = runtime.execute("SELECT * FROM sensors LIMIT 2");
        SelectResult result = (SelectResult) resultObj;

        // Raw values have dictionary IDs
        assertEquals(1.0, result.rows.get(0).get(1), 1e-9);

        // Decode should return mixed list with strings
        List<Object> decoded0 = runtime.decodeRow("sensors", result.rows.get(0));
        assertEquals(1.0, ((Number)decoded0.get(0)).doubleValue(), 1e-9);
        assertEquals("Bay A", decoded0.get(1));
        assertEquals(42.0, ((Number)decoded0.get(2)).doubleValue(), 1e-9);

        List<Object> decoded1 = runtime.decodeRow("sensors", result.rows.get(1));
        assertEquals("Bay B", decoded1.get(1));
    }

    @Test
    public void decodeRowWithAllDoubleColumnsReturnsDoubles() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
        runtime.execute("INSERT INTO ticks VALUES (42)");

        Object resultObj = runtime.execute("SELECT * FROM ticks LIMIT 1");
        SelectResult result = (SelectResult) resultObj;

        List<Object> decoded = runtime.decodeRow("ticks", result.rows.get(0));
        assertEquals(42.0, ((Number)decoded.get(0)).doubleValue(), 1e-9);
    }

    @Test
    public void decodeRowWithUnknownStreamReturnsRawDoubles() {
        List<Object> decoded = runtime.decodeRow("nonexistent", List.of(1.0, 2.0));
        assertEquals(1.0, ((Number)decoded.get(0)).doubleValue(), 1e-9);
        assertEquals(2.0, ((Number)decoded.get(1)).doubleValue(), 1e-9);
    }

    @Test
    public void decodeRowOnMaterializedViewDecodesTextColumns() {
        // Create a stream with TEXT columns, insert mixed data, then create
        // a materialized view that passes through TEXT columns.
        runtime.execute("CREATE STREAM sensors (device_id TEXT, location TEXT, value DOUBLE PRECISION)");
        runtime.insertMixed("sensors", 1000L, List.of("Pump 01", "Bay A", 42.0));
        runtime.insertMixed("sensors", 2000L, List.of("Pump 02", "Bay B", 99.0));

        // Materialized view that selects TEXT columns alongside aggregated values
        runtime.execute("CREATE MATERIALIZED VIEW sensor_output AS "
                + "SELECT device_id, location, value FROM sensors");

        // Read the view output to get raw double values (dictionary-encoded TEXT)
        Object resultObj = runtime.execute("SELECT * FROM sensor_output LIMIT 2");
        SelectResult result = (SelectResult) resultObj;
        assertFalse("Expected view output rows", result.rows.isEmpty());

        // decodeRow with the view name should decode TEXT columns back to strings
        List<Object> decoded0 = runtime.decodeRow("sensor_output", result.rows.get(0));
        assertEquals("device_id should decode to string", "Pump 01", decoded0.get(0));
        assertEquals("location should decode to string", "Bay A", decoded0.get(1));
        assertEquals(42.0, ((Number) decoded0.get(2)).doubleValue(), 1e-9);

        List<Object> decoded1 = runtime.decodeRow("sensor_output", result.rows.get(1));
        assertEquals("Pump 02", decoded1.get(0));
        assertEquals("Bay B", decoded1.get(1));
        assertEquals(99.0, ((Number) decoded1.get(2)).doubleValue(), 1e-9);
    }

    // -----------------------------------------------------------------
    // Dictionary persistence across serialize/restore
    // -----------------------------------------------------------------

    @Test
    public void dictionaryStateSurvivesSerializeRestore() {
        runtime.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)");
        runtime.insertMixed("sensors", 1000L, List.of(1.0, "Bay A", 42.0));
        runtime.insertMixed("sensors", 2000L, List.of(2.0, "Bay B", 99.0));

        // Serialize
        Map<String, String> state = runtime.serializeState();
        assertNotNull(state);

        // Create fresh runtime, restore
        RtBotSqlRuntime runtime2 = new RtBotSqlRuntime();
        runtime2.setCollectMode(true);
        runtime2.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)");
        runtime2.restoreState(state);

        // Dictionary should be restored
        StringDictionary dict = runtime2.getCatalog().lookupDictionary("sensors.location");
        assertNotNull("dictionary should survive restore", dict);
        assertEquals(2, dict.size());
        assertEquals("Bay A", dict.decode(1.0));
        assertEquals("Bay B", dict.decode(2.0));

        // New inserts should get next ID
        runtime2.insertMixed("sensors", 3000L, List.of(3.0, "Bay C", 55.0));
        assertEquals(3.0, dict.encode("Bay C"), 1e-9);  // already encoded, returns same ID

        runtime2.destroyAll();
    }

    @Test
    public void emptyDictionarySerializesCleanly() {
        runtime.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
        Map<String, String> state = runtime.serializeState();
        assertNotNull(state);

        RtBotSqlRuntime runtime2 = new RtBotSqlRuntime();
        runtime2.setCollectMode(true);
        runtime2.execute("CREATE STREAM ticks (value DOUBLE PRECISION)");
        runtime2.restoreState(state);
        // No crash, no dictionaries
        assertNull(runtime2.getCatalog().lookupDictionary("ticks.value"));
        runtime2.destroyAll();
    }

    // =================================================================
    // End-to-End Integration: Full Pipeline with TEXT Columns (Task 14)
    // =================================================================

    /**
     * E2E: TEXT column with GROUP BY on a numeric column.
     *
     * Verifies that dictionary-encoded TEXT values coexist with numeric
     * GROUP BY aggregation. The TEXT column (location) is in the source
     * stream but not in the GROUP BY key or aggregated output — it only
     * participates as a passthrough in the source rows.
     */
    @Test
    public void endToEndTextColumnWithGroupBy() {
        runtime.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)");

        runtime.insertMixed("sensors", 1000L, List.of(1.0, "Bay A", 10.0));
        runtime.insertMixed("sensors", 2000L, List.of(1.0, "Bay A", 20.0));
        runtime.insertMixed("sensors", 3000L, List.of(2.0, "Bay B", 30.0));
        runtime.insertMixed("sensors", 4000L, List.of(2.0, "Bay B", 40.0));

        // GROUP BY on numeric column device_id — TEXT column not selected
        runtime.execute(
            "CREATE MATERIALIZED VIEW avg_by_device AS "
            + "SELECT device_id, AVG(value) AS avg_val "
            + "FROM sensors GROUP BY device_id"
        );

        Object resultObj = runtime.execute("SELECT * FROM avg_by_device");
        assertTrue(resultObj instanceof SelectResult);
        SelectResult result = (SelectResult) resultObj;

        assertEquals(2, result.rows.size());

        // Build map by device_id for order-independent assertions
        int deviceIdx = result.columns.indexOf("device_id");
        int avgIdx = result.columns.indexOf("avg_val");
        assertTrue("Should have device_id column", deviceIdx >= 0);
        assertTrue("Should have avg_val column", avgIdx >= 0);

        Map<Double, Double> avgByDevice = new HashMap<>();
        for (List<Double> row : result.rows) {
            avgByDevice.put(row.get(deviceIdx), row.get(avgIdx));
        }

        // device 1: avg(10, 20) = 15.0
        assertEquals(15.0, avgByDevice.get(1.0), 1e-9);
        // device 2: avg(30, 40) = 35.0
        assertEquals(35.0, avgByDevice.get(2.0), 1e-9);
    }

    /**
     * E2E: TEXT column with subscription API.
     *
     * Verifies that raw subscription output contains dictionary IDs
     * (as doubles) for TEXT columns, and that {@code decodeRow()} correctly
     * converts them back to human-readable strings.
     */
    @Test
    public void endToEndTextColumnWithSubscription() {
        RtBotSqlRuntime subRuntime = new RtBotSqlRuntime();
        try {
            subRuntime.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)");

            CollectingListener listener = new CollectingListener();
            subRuntime.subscribe("sensors", listener);

            subRuntime.insertMixed("sensors", 1000L, List.of(1.0, "Bay A", 42.0));

            assertEquals("Subscriber should receive 1 message", 1, listener.count());

            // Raw subscription sees dictionary IDs — "Bay A" encoded as 1.0
            List<Double> rawValues = listener.values.get(0);
            assertEquals(1.0, rawValues.get(0), 1e-9);  // device_id
            assertEquals(1.0, rawValues.get(1), 1e-9);  // "Bay A" -> dictionary ID 1.0
            assertEquals(42.0, rawValues.get(2), 1e-9);  // value

            // decodeRow converts dictionary IDs back to strings
            List<Object> decoded = subRuntime.decodeRow("sensors", rawValues);
            assertEquals(1.0, ((Number) decoded.get(0)).doubleValue(), 1e-9);
            assertEquals("Bay A", decoded.get(1));
            assertEquals(42.0, ((Number) decoded.get(2)).doubleValue(), 1e-9);

            // Insert a second row with same string — should reuse dictionary ID
            subRuntime.insertMixed("sensors", 2000L, List.of(2.0, "Bay A", 99.0));
            List<Double> rawValues2 = listener.values.get(1);
            assertEquals(1.0, rawValues2.get(1), 1e-9);  // same dictionary ID

            // Insert with new string — gets next dictionary ID
            subRuntime.insertMixed("sensors", 3000L, List.of(3.0, "Bay B", 55.0));
            List<Double> rawValues3 = listener.values.get(2);
            assertEquals(2.0, rawValues3.get(1), 1e-9);  // "Bay B" -> dictionary ID 2.0

            List<Object> decoded3 = subRuntime.decodeRow("sensors", rawValues3);
            assertEquals("Bay B", decoded3.get(1));
        } finally {
            subRuntime.destroyAll();
        }
    }

    /**
     * E2E: SQL INSERT with string literal for TEXT column.
     *
     * Verifies that the C++ compiler's dictionary encoding of string literals
     * in INSERT statements flows through the JNI bridge. The compiler encodes
     * 'Bay A' to a numeric dictionary ID in the insert_payload, and the
     * {@code dictionary_updates} field syncs the mapping back to the Java
     * catalog so that subsequent INSERTs and {@code decodeRow()} work correctly.
     */
    @Test
    public void endToEndSqlInsertWithStringLiteral() {
        runtime.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)");

        // SQL INSERT with string literal — C++ compiler encodes 'Bay A' to dictionary ID
        runtime.execute("INSERT INTO sensors VALUES (1, 'Bay A', 42.0)");

        Object resultObj = runtime.execute("SELECT * FROM sensors LIMIT 1");
        assertTrue(resultObj instanceof SelectResult);
        SelectResult result = (SelectResult) resultObj;
        assertEquals(1, result.rows.size());

        // The C++ compiler encodes 'Bay A' as dictionary ID 1.0
        int locIdx = result.columns.indexOf("location");
        assertTrue("Should have location column", locIdx >= 0);
        assertEquals(1.0, result.rows.get(0).get(locIdx), 1e-9);

        // Verify other columns are correct
        int deviceIdx = result.columns.indexOf("device_id");
        int valueIdx = result.columns.indexOf("value");
        assertEquals(1.0, result.rows.get(0).get(deviceIdx), 1e-9);
        assertEquals(42.0, result.rows.get(0).get(valueIdx), 1e-9);

        // With dictionary sync, decodeRow should now return string values
        List<Object> decoded = runtime.decodeRow("sensors", result.rows.get(0));
        assertEquals("Bay A", decoded.get(locIdx));
        assertEquals(1.0, decoded.get(deviceIdx));
        assertEquals(42.0, decoded.get(valueIdx));

        // Second INSERT with same string literal — both get 1.0
        runtime.execute("INSERT INTO sensors VALUES (2, 'Bay A', 99.0)");

        Object resultObj2 = runtime.execute("SELECT * FROM sensors LIMIT 10");
        SelectResult result2 = (SelectResult) resultObj2;
        assertEquals(2, result2.rows.size());
        assertEquals(1.0, result2.rows.get(0).get(locIdx), 1e-9);
        assertEquals(1.0, result2.rows.get(1).get(locIdx), 1e-9);

        // Both rows should decode to "Bay A"
        List<Object> decoded0 = runtime.decodeRow("sensors", result2.rows.get(0));
        List<Object> decoded1 = runtime.decodeRow("sensors", result2.rows.get(1));
        assertEquals("Bay A", decoded0.get(locIdx));
        assertEquals("Bay A", decoded1.get(locIdx));
    }

    /**
     * E2E: Mixed insertMixed + SQL INSERT shows that insertMixed populates
     * the Java dictionary, enabling decodeRow() for all rows.
     *
     * <p>This is the recommended pattern: use insertMixed() for data with
     * TEXT columns when decode support is needed.
     */
    @Test
    public void endToEndInsertMixedThenSelectAndDecode() {
        runtime.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)");

        // insertMixed handles dictionary encoding on the Java side
        runtime.insertMixed("sensors", 1000L, List.of(1.0, "Bay A", 42.0));
        runtime.insertMixed("sensors", 2000L, List.of(2.0, "Bay B", 99.0));
        runtime.insertMixed("sensors", 3000L, List.of(3.0, "Bay A", 55.0));

        Object resultObj = runtime.execute("SELECT * FROM sensors LIMIT 10");
        SelectResult result = (SelectResult) resultObj;
        assertEquals(3, result.rows.size());

        int locIdx = result.columns.indexOf("location");
        // 'Bay A' always gets dictionary ID 1.0, 'Bay B' gets 2.0
        assertEquals(1.0, result.rows.get(0).get(locIdx), 1e-9);
        assertEquals(2.0, result.rows.get(1).get(locIdx), 1e-9);
        assertEquals(1.0, result.rows.get(2).get(locIdx), 1e-9);  // reused

        // decodeRow converts all three rows correctly
        List<Object> decoded0 = runtime.decodeRow("sensors", result.rows.get(0));
        assertEquals("Bay A", decoded0.get(locIdx));

        List<Object> decoded1 = runtime.decodeRow("sensors", result.rows.get(1));
        assertEquals("Bay B", decoded1.get(locIdx));

        List<Object> decoded2 = runtime.decodeRow("sensors", result.rows.get(2));
        assertEquals("Bay A", decoded2.get(locIdx));
    }

    /**
     * E2E: TEXT column serialize/restore and continue inserting.
     *
     * Verifies that string dictionaries are persisted in the serialized state
     * and correctly restored in a fresh runtime. After restore, new string
     * values get the next sequential dictionary IDs.
     */
    @Test
    public void endToEndTextColumnSerializeRestoreAndContinue() {
        runtime.execute("CREATE STREAM sensors (location TEXT, value DOUBLE PRECISION)");
        runtime.insertMixed("sensors", 1000L, List.of("Bay A", 10.0));
        runtime.insertMixed("sensors", 2000L, List.of("Bay B", 20.0));

        // Verify dictionary state before serialize
        StringDictionary dictBefore = runtime.getCatalog().lookupDictionary("sensors.location");
        assertNotNull(dictBefore);
        assertEquals(2, dictBefore.size());

        // Serialize
        Map<String, String> state = runtime.serializeState();
        assertNotNull(state);
        assertTrue("State should contain dictionary entry",
                   state.containsKey("dict:sensors.location"));

        // Fresh runtime with same schema
        RtBotSqlRuntime runtime2 = new RtBotSqlRuntime();
        runtime2.setCollectMode(true);
        try {
            runtime2.execute("CREATE STREAM sensors (location TEXT, value DOUBLE PRECISION)");
            runtime2.restoreState(state);

            // Dictionary should be restored with both entries
            StringDictionary restoredDict = runtime2.getCatalog().lookupDictionary("sensors.location");
            assertNotNull("Dictionary should survive serialize/restore", restoredDict);
            assertEquals(2, restoredDict.size());
            assertEquals("Bay A", restoredDict.decode(1.0));
            assertEquals("Bay B", restoredDict.decode(2.0));

            // Insert new data — should get next sequential ID (3.0)
            runtime2.insertMixed("sensors", 3000L, List.of("Bay C", 30.0));

            assertEquals(3, restoredDict.size());
            assertEquals("Bay C", restoredDict.decode(3.0));

            // Existing strings should still decode correctly
            assertEquals("Bay A", restoredDict.decode(1.0));
            assertEquals("Bay B", restoredDict.decode(2.0));

            // Re-inserting an existing string should reuse the same ID
            runtime2.insertMixed("sensors", 4000L, List.of("Bay A", 40.0));
            assertEquals("Dictionary should still have 3 entries (Bay A reused)",
                         3, restoredDict.size());

            // Verify the full data via SELECT + decode
            Object resultObj = runtime2.execute("SELECT * FROM sensors LIMIT 10");
            SelectResult result = (SelectResult) resultObj;
            // Only rows inserted in runtime2 (after restore), not the original ones
            assertEquals(2, result.rows.size());

            List<Object> decoded0 = runtime2.decodeRow("sensors", result.rows.get(0));
            assertEquals("Bay C", decoded0.get(0));
            assertEquals(30.0, ((Number) decoded0.get(1)).doubleValue(), 1e-9);

            List<Object> decoded1 = runtime2.decodeRow("sensors", result.rows.get(1));
            assertEquals("Bay A", decoded1.get(0));
            assertEquals(40.0, ((Number) decoded1.get(1)).doubleValue(), 1e-9);
        } finally {
            runtime2.destroyAll();
        }
    }

    @Test
    public void multiStreamSubscriberReceivesOnlyMatchingTimestamps() {
        RtBotSqlRuntime rt = new RtBotSqlRuntime();
        try {
            rt.execute("CREATE STREAM stream_a (value DOUBLE)");
            rt.execute("CREATE STREAM stream_b (value DOUBLE)");
            rt.execute(
                    "CREATE MATERIALIZED VIEW combined AS "
                    + "SELECT a.value AS val_a, b.value AS val_b "
                    + "FROM stream_a a, stream_b b"
            );

            List<List<Double>> received = new CopyOnWriteArrayList<>();
            rt.subscribe("combined", (name, ts, vals) -> received.add(new ArrayList<>(vals)));

            // Non-matching: no output
            rt.insert("stream_a", 300, Arrays.asList(10.0));
            rt.insert("stream_b", 400, Arrays.asList(20.0));
            assertEquals("Non-matching timestamps: subscriber should receive nothing",
                    0, received.size());

            // Matching: output
            rt.insert("stream_a", 1000, Arrays.asList(50.0));
            rt.insert("stream_b", 1000, Arrays.asList(60.0));
            assertEquals("Matching timestamp: subscriber should receive one message",
                    1, received.size());
        } finally {
            rt.destroyAll();
        }
    }

    /**
     * SQL INSERT with string literal syncs dictionary back to catalog.
     */
    @Test
    public void sqlInsertStringLiteralSyncsDictionaryToCatalog() {
        runtime.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)");
        runtime.execute("INSERT INTO sensors VALUES (1, 'Bay A', 42.0)");

        InMemoryCatalog cat = runtime.getCatalog();
        StringDictionary dict = cat.lookupDictionary("sensors.location");
        assertNotNull("Dictionary should exist after SQL INSERT with string literal", dict);
        assertEquals("Bay A", dict.decode(1.0));
    }

    /**
     * Consecutive SQL INSERTs with different string literals produce
     * distinct dictionary IDs and decodeRow works for all.
     */
    @Test
    public void consecutiveSqlInsertsWithDifferentStringsProduceDistinctIds() {
        runtime.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)");
        runtime.execute("INSERT INTO sensors VALUES (1, 'Bay A', 42.0)");
        runtime.execute("INSERT INTO sensors VALUES (2, 'Bay B', 99.0)");
        runtime.execute("INSERT INTO sensors VALUES (3, 'Bay A', 55.0)");

        Object resultObj = runtime.execute("SELECT * FROM sensors LIMIT 10");
        assertTrue(resultObj instanceof SelectResult);
        SelectResult result = (SelectResult) resultObj;
        assertEquals(3, result.rows.size());

        int locIdx = result.columns.indexOf("location");
        assertTrue(locIdx >= 0);

        // 'Bay A' should consistently get ID 1.0, 'Bay B' should get 2.0
        assertEquals(1.0, result.rows.get(0).get(locIdx), 1e-9);
        assertEquals(2.0, result.rows.get(1).get(locIdx), 1e-9);
        assertEquals(1.0, result.rows.get(2).get(locIdx), 1e-9);

        // decodeRow should work for all rows
        List<Object> decoded0 = runtime.decodeRow("sensors", result.rows.get(0));
        List<Object> decoded1 = runtime.decodeRow("sensors", result.rows.get(1));
        List<Object> decoded2 = runtime.decodeRow("sensors", result.rows.get(2));
        assertEquals("Bay A", decoded0.get(locIdx));
        assertEquals("Bay B", decoded1.get(locIdx));
        assertEquals("Bay A", decoded2.get(locIdx));
    }

    // =================================================================
    // Exploratory: ALIGNED STREAM pipeline validation
    // =================================================================

    /**
     * Manually execute the 7 SQL statements that the preprocessor would
     * generate for:
     *   CREATE ALIGNED STREAM input (vibration DOUBLE, bearing_temp DOUBLE) BIN(1s)
     *
     * with ts_units_per_second = 1000 (milliseconds).
     *
     * Then feed individual messages at irregular timestamps and observe
     * what comes out of the combining view ("input").
     *
     * Bin size = 1000ms. Grid: 0, 1000, 2000, 3000, ...
     *
     * Vibration stream (5 samples per bin, irregular spacing):
     *   bin 0 (t<1000):      t=50 v=8, t=200 v=12, t=400 v=10, t=600 v=14, t=900 v=6
     *                        → avg = (8+12+10+14+6)/5 = 10.0
     *   bin 1 (1000≤t<2000): t=1050 v=20, t=1300 v=22, t=1500 v=28, t=1800 v=30
     *                        → avg = (20+22+28+30)/4 = 25.0
     *   bin 2 (2000≤t<3000): t=2100 v=30, t=2400 v=40, t=2700 v=35
     *                        → avg = (30+40+35)/3 = 35.0
     *   bin 3 trigger:       t=3100 v=999 (just to flush bin 2)
     *
     * Bearing_temp stream (different sample count per bin):
     *   bin 0 (t<1000):      t=100 v=68, t=300 v=72, t=500 v=74, t=700 v=78, t=850 v=76, t=950 v=82
     *                        → avg = (68+72+74+78+76+82)/6 = 75.0
     *   bin 1 (1000≤t<2000): t=1100 v=85, t=1400 v=90, t=1700 v=95
     *                        → avg = (85+90+95)/3 = 90.0
     *   bin 2 (2000≤t<3000): t=2200 v=96, t=2500 v=100, t=2800 v=104
     *                        → avg = (96+100+104)/3 = 100.0
     *   bin 3 trigger:       t=3200 v=999 (just to flush bin 2)
     *
     * Expected combined output (after resampler initialization + TIMESHIFT correction):
     *   t=1000: vibration=10.0, bearing_temp=75.0  (bin 0 averages)
     *   t=2000: vibration=25.0, bearing_temp=90.0  (bin 1 averages)
     */
    @Test
    public void alignedStreamPipelineManualExecution() {
        // -- Step 1: Execute the 7 statements the preprocessor would generate --
        // (bin_units = 1000 for BIN(1s) with ts_units_per_second=1000)

        // Per-column streams
        runtime.execute("CREATE STREAM vibration (value DOUBLE)");
        runtime.execute("CREATE STREAM bearing_temp (value DOUBLE)");

        // Binning views
        runtime.execute(
            "CREATE VIEW vibration_bin AS SELECT AVG(value) AS vibration "
            + "FROM vibration GROUP BY FLOOR(TS() / 1000)"
        );
        runtime.execute(
            "CREATE VIEW bearing_temp_bin AS SELECT AVG(value) AS bearing_temp "
            + "FROM bearing_temp GROUP BY FLOOR(TS() / 1000)"
        );

        // Resampler views (with TIMESHIFT to correct one-bin latency)
        runtime.execute(
            "CREATE VIEW vibration_rs AS SELECT TIMESHIFT(RESAMPLE_CONSTANT(vibration, 1000), -1000) AS vibration "
            + "FROM vibration_bin"
        );
        runtime.execute(
            "CREATE VIEW bearing_temp_rs AS SELECT TIMESHIFT(RESAMPLE_CONSTANT(bearing_temp, 1000), -1000) AS bearing_temp "
            + "FROM bearing_temp_bin"
        );

        // Combining view (cross-join)
        runtime.execute(
            "CREATE VIEW input AS SELECT vibration, bearing_temp "
            + "FROM vibration_rs, bearing_temp_rs"
        );

        // Subscribe to all views to see every message flowing through
        CollectingListener vibBinListener = new CollectingListener();
        CollectingListener tempBinListener = new CollectingListener();
        CollectingListener vibRsListener = new CollectingListener();
        CollectingListener tempRsListener = new CollectingListener();
        CollectingListener inputListener = new CollectingListener();
        runtime.subscribe("vibration_bin", vibBinListener);
        runtime.subscribe("bearing_temp_bin", tempBinListener);
        runtime.subscribe("vibration_rs", vibRsListener);
        runtime.subscribe("bearing_temp_rs", tempRsListener);
        runtime.subscribe("input", inputListener);

        // -- Step 2: Feed messages one at a time --

        // ---- Bin 0: fill both streams with many samples ----
        // Vibration: 5 samples, avg = (8+12+10+14+6)/5 = 10.0
        runtime.insert("vibration", 50L,  Arrays.asList(8.0));
        runtime.insert("vibration", 200L, Arrays.asList(12.0));
        runtime.insert("vibration", 400L, Arrays.asList(10.0));
        runtime.insert("vibration", 600L, Arrays.asList(14.0));
        runtime.insert("vibration", 900L, Arrays.asList(6.0));

        // Bearing_temp: 6 samples, avg = (68+72+74+78+76+82)/6 = 75.0
        runtime.insert("bearing_temp", 100L, Arrays.asList(68.0));
        runtime.insert("bearing_temp", 300L, Arrays.asList(72.0));
        runtime.insert("bearing_temp", 500L, Arrays.asList(74.0));
        runtime.insert("bearing_temp", 700L, Arrays.asList(78.0));
        runtime.insert("bearing_temp", 850L, Arrays.asList(76.0));
        runtime.insert("bearing_temp", 950L, Arrays.asList(82.0));

        // Still in bin 0 — nothing emitted yet
        assertEquals("No _bin output yet (still in bin 0)",
                     0, vibBinListener.count() + tempBinListener.count());

        // ---- Bin 1: first message crosses into bin 1, flushing bin 0 ----
        // Vibration bin 1: 4 samples, avg = (20+22+28+30)/4 = 25.0
        runtime.insert("vibration", 1050L, Arrays.asList(20.0));  // triggers bin 0 flush
        assertEquals("vibration_bin: bin 0 flushed", 1, vibBinListener.count());
        assertEquals("Resampler init (first msg): no rs output", 0, vibRsListener.count());

        runtime.insert("vibration", 1300L, Arrays.asList(22.0));
        runtime.insert("vibration", 1500L, Arrays.asList(28.0));
        runtime.insert("vibration", 1800L, Arrays.asList(30.0));

        // Bearing_temp bin 1: 3 samples, avg = (85+90+95)/3 = 90.0
        runtime.insert("bearing_temp", 1100L, Arrays.asList(85.0));  // triggers bin 0 flush
        assertEquals("bearing_temp_bin: bin 0 flushed", 1, tempBinListener.count());
        assertEquals("Resampler init (first msg): no rs output", 0, tempRsListener.count());

        runtime.insert("bearing_temp", 1400L, Arrays.asList(90.0));
        runtime.insert("bearing_temp", 1700L, Arrays.asList(95.0));

        // No combined output yet — resamplers only initialized
        assertEquals("No combined output yet", 0, inputListener.count());

        // ---- Bin 2: crosses into bin 2, flushing bin 1 ----
        // Vibration bin 2: 3 samples, avg = (30+40+35)/3 = 35.0
        runtime.insert("vibration", 2100L, Arrays.asList(30.0));  // triggers bin 1 flush
        assertEquals("vibration_bin: 2 flushes now", 2, vibBinListener.count());
        assertEquals("vibration_rs emits at t=1000", 1, vibRsListener.count());

        runtime.insert("vibration", 2400L, Arrays.asList(40.0));
        runtime.insert("vibration", 2700L, Arrays.asList(35.0));

        // Bearing_temp bin 2: 3 samples, avg = (96+100+104)/3 = 100.0
        runtime.insert("bearing_temp", 2200L, Arrays.asList(96.0));  // triggers bin 1 flush
        assertEquals("bearing_temp_bin: 2 flushes now", 2, tempBinListener.count());
        assertEquals("bearing_temp_rs emits at t=1000", 1, tempRsListener.count());

        runtime.insert("bearing_temp", 2500L, Arrays.asList(100.0));
        runtime.insert("bearing_temp", 2800L, Arrays.asList(104.0));

        // -- First combined output: t=1000, bin 0 averages --
        assertEquals("First combined output", 1, inputListener.count());
        assertEquals(1000L, (long) inputListener.timestamps.get(0));
        assertEquals("vibration bin 0 avg", 10.0, inputListener.values.get(0).get(0), 1e-9);
        assertEquals("bearing_temp bin 0 avg", 75.0, inputListener.values.get(0).get(1), 1e-9);

        // ---- Bin 3 trigger: flush bin 2 ----
        runtime.insert("vibration", 3100L, Arrays.asList(999.0));
        runtime.insert("bearing_temp", 3200L, Arrays.asList(999.0));

        // -- Second combined output: t=2000, bin 1 averages --
        assertEquals("Two combined outputs total", 2, inputListener.count());
        assertEquals(2000L, (long) inputListener.timestamps.get(1));
        assertEquals("vibration bin 1 avg", 25.0, inputListener.values.get(1).get(0), 1e-9);
        assertEquals("bearing_temp bin 1 avg", 90.0, inputListener.values.get(1).get(1), 1e-9);

        // -- Print full trace --
        System.out.println("\n=== ALIGNED STREAM PIPELINE — FULL TRACE ===");
        System.out.println("vibration_bin (" + vibBinListener.count() + " outputs):");
        for (int i = 0; i < vibBinListener.count(); i++) {
            System.out.println("  t=" + vibBinListener.timestamps.get(i)
                + "  avg=" + vibBinListener.values.get(i));
        }
        System.out.println("bearing_temp_bin (" + tempBinListener.count() + " outputs):");
        for (int i = 0; i < tempBinListener.count(); i++) {
            System.out.println("  t=" + tempBinListener.timestamps.get(i)
                + "  avg=" + tempBinListener.values.get(i));
        }
        System.out.println("vibration_rs (" + vibRsListener.count() + " outputs):");
        for (int i = 0; i < vibRsListener.count(); i++) {
            System.out.println("  t=" + vibRsListener.timestamps.get(i)
                + "  val=" + vibRsListener.values.get(i));
        }
        System.out.println("bearing_temp_rs (" + tempRsListener.count() + " outputs):");
        for (int i = 0; i < tempRsListener.count(); i++) {
            System.out.println("  t=" + tempRsListener.timestamps.get(i)
                + "  val=" + tempRsListener.values.get(i));
        }
        System.out.println("input — combined (" + inputListener.count() + " outputs):");
        for (int i = 0; i < inputListener.count(); i++) {
            System.out.println("  t=" + inputListener.timestamps.get(i)
                + "  [vibration, bearing_temp]=" + inputListener.values.get(i));
        }
    }

    // =================================================================
    // Sugar syntax: CREATE ALIGNED STREAM ... BIN() end-to-end
    // =================================================================

    /**
     * End-to-end test for the sugar syntax:
     *   CREATE ALIGNED STREAM input (vibration DOUBLE, bearing_temp DOUBLE) BIN(1s)
     *
     * Unlike {@link #alignedStreamPipelineManualExecution()} which manually
     * executes 7 separate statements with ts_units_per_second=1000 (ms), this
     * test issues a single {@code execute()} call with the real default
     * timescale (1,000,000 μs) and lets the preprocessor expand it.
     *
     * With tsUnitsPerSecond=1,000,000 and BIN(1s), bin_units = 1,000,000.
     * Grid: 0, 1_000_000, 2_000_000, 3_000_000, ...
     *
     * Vibration stream (5 samples per bin, irregular spacing):
     *   bin 0 (t<1,000,000μs):          t=50000 v=8, t=200000 v=12, t=400000 v=10,
     *                                    t=600000 v=14, t=900000 v=6
     *                                    → avg = (8+12+10+14+6)/5 = 10.0
     *   bin 1 (1,000,000≤t<2,000,000):  t=1050000 v=20, t=1300000 v=22,
     *                                    t=1500000 v=28, t=1800000 v=30
     *                                    → avg = (20+22+28+30)/4 = 25.0
     *   bin 2 (2,000,000≤t<3,000,000):  t=2100000 v=30, t=2400000 v=40, t=2700000 v=35
     *                                    → avg = (30+40+35)/3 = 35.0
     *   bin 3 trigger:                   t=3100000 v=999 (flush bin 2)
     *
     * Bearing_temp stream (different sample count per bin):
     *   bin 0 (t<1,000,000μs):          t=100000 v=68, t=300000 v=72, t=500000 v=74,
     *                                    t=700000 v=78, t=850000 v=76, t=950000 v=82
     *                                    → avg = (68+72+74+78+76+82)/6 = 75.0
     *   bin 1 (1,000,000≤t<2,000,000):  t=1100000 v=85, t=1400000 v=90, t=1700000 v=95
     *                                    → avg = (85+90+95)/3 = 90.0
     *   bin 2 (2,000,000≤t<3,000,000):  t=2200000 v=96, t=2500000 v=100, t=2800000 v=104
     *                                    → avg = (96+100+104)/3 = 100.0
     *   bin 3 trigger:                   t=3200000 v=999 (flush bin 2)
     *
     * Expected combined output (after resampler initialization + TIMESHIFT correction):
     *   t=1,000,000: vibration=10.0, bearing_temp=75.0  (bin 0 averages)
     *   t=2,000,000: vibration=25.0, bearing_temp=90.0  (bin 1 averages)
     */
    @Test
    public void alignedStreamSugarSyntax() {
        // -- Step 1: Single sugar-syntax call (expands to 7 internal statements) --
        //
        // The preprocessor expands:
        //   CREATE ALIGNED STREAM input (vibration DOUBLE, bearing_temp DOUBLE) BIN(1s)
        // into:
        //   1. CREATE STREAM vibration (value DOUBLE)
        //   2. CREATE STREAM bearing_temp (value DOUBLE)
        //   3. CREATE VIEW vibration_bin AS SELECT AVG(value) AS vibration
        //        FROM vibration GROUP BY FLOOR(TS() / 1000000)
        //   4. CREATE VIEW bearing_temp_bin AS SELECT AVG(value) AS bearing_temp
        //        FROM bearing_temp GROUP BY FLOOR(TS() / 1000000)
        //   5. CREATE VIEW vibration_rs AS SELECT TIMESHIFT(RESAMPLE_CONSTANT(vibration, 1000000), -1000000) AS vibration
        //        FROM vibration_bin
        //   6. CREATE VIEW bearing_temp_rs AS SELECT TIMESHIFT(RESAMPLE_CONSTANT(bearing_temp, 1000000), -1000000) AS bearing_temp
        //        FROM bearing_temp_bin
        //   7. CREATE MATERIALIZED VIEW input AS SELECT vibration, bearing_temp
        //        FROM vibration_rs, bearing_temp_rs

        RtBotSqlRuntime rt = new RtBotSqlRuntime();
        try {
            rt.execute(
                "CREATE ALIGNED STREAM input (vibration DOUBLE, bearing_temp DOUBLE) BIN(1s)"
            );

            // -- Step 2: Verify catalog state --
            // 2 streams created by the expansion
            assertNotNull("vibration stream should exist",
                          rt.getCatalog().lookupStream("vibration"));
            assertNotNull("bearing_temp stream should exist",
                          rt.getCatalog().lookupStream("bearing_temp"));

            // 5 views created by the expansion
            assertNotNull("vibration_bin view should exist",
                          rt.getCatalog().lookupView("vibration_bin"));
            assertNotNull("bearing_temp_bin view should exist",
                          rt.getCatalog().lookupView("bearing_temp_bin"));
            assertNotNull("vibration_rs view should exist",
                          rt.getCatalog().lookupView("vibration_rs"));
            assertNotNull("bearing_temp_rs view should exist",
                          rt.getCatalog().lookupView("bearing_temp_rs"));
            assertNotNull("input combining materialized view should exist",
                          rt.getCatalog().lookupView("input"));

            // -- Step 3: Subscribe to intermediate views and the combining view --
            CollectingListener vibBinListener = new CollectingListener();
            CollectingListener tempBinListener = new CollectingListener();
            CollectingListener vibRsListener = new CollectingListener();
            CollectingListener tempRsListener = new CollectingListener();
            CollectingListener inputListener = new CollectingListener();
            rt.subscribe("vibration_bin", vibBinListener);
            rt.subscribe("bearing_temp_bin", tempBinListener);
            rt.subscribe("vibration_rs", vibRsListener);
            rt.subscribe("bearing_temp_rs", tempRsListener);
            rt.subscribe("input", inputListener);

            // -- Step 4: Insert data into per-column streams --
            //
            // Java default tsUnitsPerSecond=1,000,000 (microseconds).
            // BIN(1s) → bin_units = 1,000,000.  Grid: 0, 1000000, 2000000, ...

            // ---- Bin 0: fill both streams with many samples ----
            //
            // Vibration stream inputs (bin 0):
            //   (t=50000μs, [8.0]) → (t=200000μs, [12.0]) → (t=400000μs, [10.0])
            //   → (t=600000μs, [14.0]) → (t=900000μs, [6.0]) → avg=10.0
            //
            // Pipeline: vibration → vibration_bin → vibration_rs → input
            //   vibration:     (50000, [8.0]), (200000, [12.0]), (400000, [10.0]), (600000, [14.0]), (900000, [6.0])
            //   vibration_bin: (1050000, [10.0])  ← flushed when bin 1 starts
            //   vibration_rs:  (1000000, [10.0])  ← TIMESHIFT(-1000000) corrects the resampler latency
            //   input:         (1000000, [10.0, 75.0])  ← cross-join of vibration_rs and bearing_temp_rs
            rt.insert("vibration",  50000L, Arrays.asList(8.0));
            rt.insert("vibration", 200000L, Arrays.asList(12.0));
            rt.insert("vibration", 400000L, Arrays.asList(10.0));
            rt.insert("vibration", 600000L, Arrays.asList(14.0));
            rt.insert("vibration", 900000L, Arrays.asList(6.0));

            // Bearing_temp stream inputs (bin 0):
            //   (t=100000μs, [68.0]) → (t=300000μs, [72.0]) → (t=500000μs, [74.0])
            //   → (t=700000μs, [78.0]) → (t=850000μs, [76.0]) → (t=950000μs, [82.0]) → avg=75.0
            //
            // Pipeline: bearing_temp → bearing_temp_bin → bearing_temp_rs → input
            //   bearing_temp:     (100000, [68.0]), (300000, [72.0]), (500000, [74.0]),
            //                     (700000, [78.0]), (850000, [76.0]), (950000, [82.0])
            //   bearing_temp_bin: (1100000, [75.0])  ← flushed when bin 1 starts
            //   bearing_temp_rs:  (1000000, [75.0])  ← TIMESHIFT(-1000000) corrects the resampler latency
            //   input:            (1000000, [10.0, 75.0])  ← cross-join with vibration_rs
            rt.insert("bearing_temp", 100000L, Arrays.asList(68.0));
            rt.insert("bearing_temp", 300000L, Arrays.asList(72.0));
            rt.insert("bearing_temp", 500000L, Arrays.asList(74.0));
            rt.insert("bearing_temp", 700000L, Arrays.asList(78.0));
            rt.insert("bearing_temp", 850000L, Arrays.asList(76.0));
            rt.insert("bearing_temp", 950000L, Arrays.asList(82.0));

            // Still in bin 0 — nothing emitted yet
            assertEquals("No _bin output yet (still in bin 0)",
                         0, vibBinListener.count() + tempBinListener.count());

            // ---- Bin 1: first message crosses into bin 1, flushing bin 0 ----
            //
            // Vibration bin 1 inputs:
            //   (t=1050000μs, [20.0]) → (t=1300000μs, [22.0]) → (t=1500000μs, [28.0])
            //   → (t=1800000μs, [30.0]) → avg=25.0
            //
            // Pipeline trace for vibration (bin 0 flush):
            //   vibration:     (1050000, [20.0])  ← first sample in bin 1 triggers bin 0 flush
            //   vibration_bin: (1050000, [10.0])  ← bin 0 average emitted
            //   vibration_rs:  (1000000, [10.0])  ← snap-first resampler snaps the first
            //                                        message's value to the current grid point
            rt.insert("vibration", 1050000L, Arrays.asList(20.0));
            assertEquals("vibration_bin: bin 0 flushed", 1, vibBinListener.count());
            assertEquals("Resampler snap-first: 1 rs output at bin 0 grid",
                         1, vibRsListener.count());

            rt.insert("vibration", 1300000L, Arrays.asList(22.0));
            rt.insert("vibration", 1500000L, Arrays.asList(28.0));
            rt.insert("vibration", 1800000L, Arrays.asList(30.0));

            // Bearing_temp bin 1 inputs:
            //   (t=1100000μs, [85.0]) → (t=1400000μs, [90.0]) → (t=1700000μs, [95.0]) → avg=90.0
            //
            // Pipeline trace for bearing_temp (bin 0 flush):
            //   bearing_temp:     (1100000, [85.0])  ← first sample in bin 1 triggers bin 0 flush
            //   bearing_temp_bin: (1100000, [75.0])  ← bin 0 average emitted
            //   bearing_temp_rs:  (1000000, [75.0])  ← snap-first resampler snaps the first
            //                                           message's value to the current grid point
            rt.insert("bearing_temp", 1100000L, Arrays.asList(85.0));
            assertEquals("bearing_temp_bin: bin 0 flushed", 1, tempBinListener.count());
            assertEquals("Resampler snap-first: 1 rs output at bin 0 grid",
                         1, tempRsListener.count());

            rt.insert("bearing_temp", 1400000L, Arrays.asList(90.0));
            rt.insert("bearing_temp", 1700000L, Arrays.asList(95.0));

            // Snap-first resamplers both already emitted at t=1,000,000 above,
            // so the cross-join over vibration_rs x bearing_temp_rs fires the
            // first combined output at t=1,000,000.
            assertEquals("Combined output for bin 0 already fired",
                         1, inputListener.count());

            // ---- Bin 2: crosses into bin 2, flushing bin 1 ----
            //
            // Vibration bin 2 inputs:
            //   (t=2100000μs, [30.0]) → (t=2400000μs, [40.0]) → (t=2700000μs, [35.0]) → avg=35.0
            //
            // Pipeline trace for vibration (bin 1 flush → resampler emits):
            //   vibration:     (2100000, [30.0])    ← first sample in bin 2 triggers bin 1 flush
            //   vibration_bin: (2100000, [25.0])    ← bin 1 average emitted (2nd bin output)
            //   vibration_rs:  (2000000, [25.0])    ← snap-first resampler emits at grid t=2,000,000
            rt.insert("vibration", 2100000L, Arrays.asList(30.0));
            assertEquals("vibration_bin: 2 flushes now", 2, vibBinListener.count());
            assertEquals("vibration_rs 2nd emit at t=2,000,000", 2, vibRsListener.count());

            rt.insert("vibration", 2400000L, Arrays.asList(40.0));
            rt.insert("vibration", 2700000L, Arrays.asList(35.0));

            // Bearing_temp bin 2 inputs:
            //   (t=2200000μs, [96.0]) → (t=2500000μs, [100.0]) → (t=2800000μs, [104.0]) → avg=100.0
            //
            // Pipeline trace for bearing_temp (bin 1 flush → resampler emits → cross-join fires):
            //   bearing_temp:     (2200000, [96.0])   ← first sample in bin 2 triggers bin 1 flush
            //   bearing_temp_bin: (2200000, [90.0])   ← bin 1 average emitted (2nd bin output)
            //   bearing_temp_rs:  (2000000, [90.0])   ← snap-first resampler emits at grid t=2,000,000
            //   input:            (2000000, [25.0, 90.0])  ← cross-join fires at t=2,000,000
            rt.insert("bearing_temp", 2200000L, Arrays.asList(96.0));
            assertEquals("bearing_temp_bin: 2 flushes now", 2, tempBinListener.count());
            assertEquals("bearing_temp_rs 2nd emit at t=2,000,000", 2, tempRsListener.count());

            rt.insert("bearing_temp", 2500000L, Arrays.asList(100.0));
            rt.insert("bearing_temp", 2800000L, Arrays.asList(104.0));

            // Snap-first resamplers cross-joined twice by now: at t=1,000,000
            // (bin 0 averages) and t=2,000,000 (bin 1 averages).
            assertEquals("Two combined outputs so far", 2, inputListener.count());
            assertEquals(1000000L, (long) inputListener.timestamps.get(0));
            assertEquals("vibration bin 0 avg", 10.0,
                         inputListener.values.get(0).get(0), 1e-9);
            assertEquals("bearing_temp bin 0 avg", 75.0,
                         inputListener.values.get(0).get(1), 1e-9);
            assertEquals(2000000L, (long) inputListener.timestamps.get(1));
            assertEquals("vibration bin 1 avg", 25.0,
                         inputListener.values.get(1).get(0), 1e-9);
            assertEquals("bearing_temp bin 1 avg", 90.0,
                         inputListener.values.get(1).get(1), 1e-9);

            // ---- Bin 3 trigger: flush bin 2 ----
            //
            // Pipeline trace (bin 2 flush → snap-first resampler emits → cross-join fires):
            //   vibration:     (3100000, [999.0])     ← triggers bin 2 flush
            //   vibration_bin: (3100000, [35.0])      ← bin 2 average
            //   vibration_rs:  (3000000, [35.0])      ← snap-first emits at grid t=3,000,000
            //
            //   bearing_temp:     (3200000, [999.0])  ← triggers bin 2 flush
            //   bearing_temp_bin: (3200000, [100.0])  ← bin 2 average
            //   bearing_temp_rs:  (3000000, [100.0])  ← snap-first emits at grid t=3,000,000
            //   input:            (3000000, [35.0, 100.0])  ← cross-join fires
            rt.insert("vibration",    3100000L, Arrays.asList(999.0));
            rt.insert("bearing_temp", 3200000L, Arrays.asList(999.0));

            // -- Third combined output: t=3,000,000, bin 2 averages --
            assertEquals("Three combined outputs total", 3, inputListener.count());
            assertEquals(3000000L, (long) inputListener.timestamps.get(2));
            assertEquals("vibration bin 2 avg", 35.0,
                         inputListener.values.get(2).get(0), 1e-9);
            assertEquals("bearing_temp bin 2 avg", 100.0,
                         inputListener.values.get(2).get(1), 1e-9);

            // -- Print full trace --
            System.out.println("\n=== ALIGNED STREAM SUGAR SYNTAX — FULL TRACE ===");
            System.out.println("vibration_bin (" + vibBinListener.count() + " outputs):");
            for (int i = 0; i < vibBinListener.count(); i++) {
                System.out.println("  t=" + vibBinListener.timestamps.get(i)
                    + "  avg=" + vibBinListener.values.get(i));
            }
            System.out.println("bearing_temp_bin (" + tempBinListener.count() + " outputs):");
            for (int i = 0; i < tempBinListener.count(); i++) {
                System.out.println("  t=" + tempBinListener.timestamps.get(i)
                    + "  avg=" + tempBinListener.values.get(i));
            }
            System.out.println("vibration_rs (" + vibRsListener.count() + " outputs):");
            for (int i = 0; i < vibRsListener.count(); i++) {
                System.out.println("  t=" + vibRsListener.timestamps.get(i)
                    + "  val=" + vibRsListener.values.get(i));
            }
            System.out.println("bearing_temp_rs (" + tempRsListener.count() + " outputs):");
            for (int i = 0; i < tempRsListener.count(); i++) {
                System.out.println("  t=" + tempRsListener.timestamps.get(i)
                    + "  val=" + tempRsListener.values.get(i));
            }
            System.out.println("input — combined (" + inputListener.count() + " outputs):");
            for (int i = 0; i < inputListener.count(); i++) {
                System.out.println("  t=" + inputListener.timestamps.get(i)
                    + "  [vibration, bearing_temp]=" + inputListener.values.get(i));
            }
        } finally {
            rt.destroyAll();
        }
    }

    /**
     * Verify that SET TIMESCALE changes the bin units for subsequent
     * CREATE ALIGNED STREAM ... BIN() calls.
     *
     * Default tsUnitsPerSecond = 1,000,000 (μs).
     * After SET TIMESCALE ms → tsUnitsPerSecond = 1,000.
     * BIN(1s) with ms timescale → bin_units = 1,000 (not 1,000,000).
     *
     * We verify this indirectly: with ms timescale, bin boundaries are at
     * t=1000, 2000, ... so samples with t<1000 are in bin 0, and a sample
     * at t>=1000 flushes bin 0.
     */
    @Test
    public void setTimescaleChangesUnits() {
        RtBotSqlRuntime rt = new RtBotSqlRuntime();
        try {
            // Change timescale from default μs to ms
            rt.execute("SET TIMESCALE ms");

            // Now BIN(1s) should produce bin_units=1000 (ms, not 1_000_000 μs)
            rt.execute(
                "CREATE ALIGNED STREAM sensor (temp DOUBLE) BIN(1s)"
            );

            // Verify catalog was populated
            assertNotNull("temp stream should exist",
                          rt.getCatalog().lookupStream("temp"));
            assertNotNull("temp_bin view should exist",
                          rt.getCatalog().lookupView("temp_bin"));
            assertNotNull("sensor combining view should exist",
                          rt.getCatalog().lookupView("sensor"));

            CollectingListener binListener = new CollectingListener();
            rt.subscribe("temp_bin", binListener);

            // Insert samples in bin 0 (t < 1000ms): avg = (20+40)/2 = 30.0
            //   temp: (100, [20.0]), (500, [40.0])
            //   temp_bin: <no output yet, still accumulating>
            rt.insert("temp", 100L, Arrays.asList(20.0));
            rt.insert("temp", 500L, Arrays.asList(40.0));
            assertEquals("Still in bin 0, no flush", 0, binListener.count());

            // First sample in bin 1 (t >= 1000ms) triggers bin 0 flush
            //   temp: (1100, [60.0])  ← crosses into bin 1
            //   temp_bin: (1100, [30.0])  ← bin 0 average flushed
            rt.insert("temp", 1100L, Arrays.asList(60.0));
            assertEquals("Bin 0 should have flushed at t=1000 boundary", 1, binListener.count());
            assertEquals("Bin 0 avg = (20+40)/2 = 30.0",
                         30.0, binListener.values.get(0).get(0), 1e-9);

            // If timescale were still μs, bin_units would be 1_000_000 and
            // the sample at t=1100 would still be in bin 0 (no flush).
            // The fact that we got a flush proves SET TIMESCALE took effect.
        } finally {
            rt.destroyAll();
        }
    }

    /**
     * Mixed path: insertMixed + SQL INSERT on the same stream.
     * Dictionary IDs must stay consistent across both insert paths.
     */
    @Test
    public void mixedInsertPathsMaintainDictionaryCoherence() {
        runtime.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)");

        // insertMixed first — Java dictionary gets Bay A=1.0
        runtime.insertMixed("sensors", 1000L, List.of(1.0, "Bay A", 42.0));

        // SQL INSERT second — C++ compiler should get the existing dictionary
        // via catalog snapshot and assign Bay B=2.0
        runtime.execute("INSERT INTO sensors VALUES (2, 'Bay B', 99.0)");

        // insertMixed again — Java dictionary should know both
        runtime.insertMixed("sensors", 3000L, List.of(3.0, "Bay A", 55.0));

        Object resultObj = runtime.execute("SELECT * FROM sensors LIMIT 10");
        SelectResult result = (SelectResult) resultObj;
        assertEquals(3, result.rows.size());

        int locIdx = result.columns.indexOf("location");
        assertEquals(1.0, result.rows.get(0).get(locIdx), 1e-9);  // Bay A
        assertEquals(2.0, result.rows.get(1).get(locIdx), 1e-9);  // Bay B
        assertEquals(1.0, result.rows.get(2).get(locIdx), 1e-9);  // Bay A

        // All rows should decode correctly
        assertEquals("Bay A", runtime.decodeRow("sensors", result.rows.get(0)).get(locIdx));
        assertEquals("Bay B", runtime.decodeRow("sensors", result.rows.get(1)).get(locIdx));
        assertEquals("Bay A", runtime.decodeRow("sensors", result.rows.get(2)).get(locIdx));
    }
}

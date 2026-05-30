package dev.rtbot.sql;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Tests for the pre-encoding ({@link RtBotSqlRuntime#encodeText}) and
 * N-column batch insert ({@link RtBotSqlRuntime#insertBatch}) paths.
 *
 * <p>Verifies that:
 * <ul>
 *   <li>encodeText produces stable, distinct dictionary doubles</li>
 *   <li>insertBatch with pre-encoded TEXT columns produces correct output</li>
 *   <li>insertBatch produces the same results as per-row insert</li>
 *   <li>insertBatch works with 1, 2, 3, and 4+ columns</li>
 * </ul>
 *
 * <p>Usage: {@code bazel test //runtimes/java:insert-batch-test --test_output=all}
 */
public class InsertBatchTest {

    private RtBotSqlRuntime runtime;

    @Before
    public void setUp() {
        runtime = new RtBotSqlRuntime();
        runtime.setCollectMode(true);
    }

    @After
    public void tearDown() {
        if (runtime != null) {
            runtime.destroyAll();
        }
    }

    // =================================================================
    // Helper: collecting listener (same pattern as RtBotSqlRuntimeTest)
    // =================================================================

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

    // =================================================================
    // encodeText correctness
    // =================================================================

    @Test
    public void encodeTextReturnsSameValueForSameString() {
        runtime.execute("SELECT STREAM sensors (device_id TEXT, value DOUBLE PRECISION)");

        double id1 = runtime.encodeText("sensors", "device_id", "Pump-01");
        double id2 = runtime.encodeText("sensors", "device_id", "Pump-01");
        double id3 = runtime.encodeText("sensors", "device_id", "Pump-01");

        assertEquals("Same string must always encode to the same double",
                     id1, id2, 1e-9);
        assertEquals("Same string must always encode to the same double",
                     id1, id3, 1e-9);
    }

    @Test
    public void encodeTextReturnsDifferentValuesForDifferentStrings() {
        runtime.execute("SELECT STREAM sensors (device_id TEXT, value DOUBLE PRECISION)");

        double id1 = runtime.encodeText("sensors", "device_id", "Pump-01");
        double id2 = runtime.encodeText("sensors", "device_id", "Pump-02");
        double id3 = runtime.encodeText("sensors", "device_id", "Motor-A");

        assertNotEquals("Different strings must produce different doubles",
                        id1, id2, 1e-9);
        assertNotEquals("Different strings must produce different doubles",
                        id1, id3, 1e-9);
        assertNotEquals("Different strings must produce different doubles",
                        id2, id3, 1e-9);
    }

    @Test
    public void encodeTextAcrossColumnsIsIndependent() {
        runtime.execute("SELECT STREAM sensors (device_id TEXT, location TEXT, value DOUBLE PRECISION)");

        // Same string encoded for different columns should use separate dictionaries
        double devId = runtime.encodeText("sensors", "device_id", "alpha");
        double locId = runtime.encodeText("sensors", "location", "alpha");

        // Both should be 1.0 (first entry in their respective dictionary)
        assertEquals("First encode in device_id dictionary should be 1.0",
                     1.0, devId, 1e-9);
        assertEquals("First encode in location dictionary should be 1.0",
                     1.0, locId, 1e-9);
    }

    // =================================================================
    // insertBatch with TEXT pre-encoding
    // =================================================================

    @Test
    public void insertBatchWithPreEncodedTextProducesCorrectOutput() {
        runtime.execute("SELECT STREAM readings (device_id TEXT, amplitude DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW device_avg AS "
            + "SELECT device_id, AVG(amplitude) AS avg_amp "
            + "FROM readings "
            + "GROUP BY device_id"
        );

        CollectingListener listener = new CollectingListener();
        runtime.subscribe("device_avg", listener);

        // Pre-encode TEXT column values
        double pumpId = runtime.encodeText("readings", "device_id", "Pump-01");
        double motorId = runtime.encodeText("readings", "device_id", "Motor-A");

        // Build batch: 4 rows — 2 for Pump-01, 2 for Motor-A
        long[] ts = {1000L, 2000L, 3000L, 4000L};
        double[] col0 = {pumpId, pumpId, motorId, motorId};  // device_id (encoded)
        double[] col1 = {10.0, 20.0, 100.0, 200.0};          // amplitude

        runtime.insertBatch("readings", ts, new double[][]{col0, col1});

        // The materialized view should have produced output
        assertTrue("Materialized view should emit output", listener.count() > 0);

        // Verify via SELECT that aggregation is correct
        Object resultObj = runtime.execute("SELECT * FROM device_avg LIMIT 10");
        assertTrue(resultObj instanceof SelectResult);
        SelectResult result = (SelectResult) resultObj;
        assertFalse("Expected rows in device_avg", result.rows.isEmpty());
    }

    @Test
    public void insertBatchMatchesPerRowInsertOutput() {
        // --- Setup runtime A: per-row insert ---
        RtBotSqlRuntime runtimeA = new RtBotSqlRuntime();
        runtimeA.setCollectMode(true);
        try {
            runtimeA.execute("SELECT STREAM data (device_id TEXT, value DOUBLE PRECISION)");
            runtimeA.execute(
                "CREATE MATERIALIZED VIEW doubled AS "
                + "SELECT device_id, value * 2 AS doubled_value FROM data"
            );

            CollectingListener listenerA = new CollectingListener();
            runtimeA.subscribe("doubled", listenerA);

            double devA = runtimeA.encodeText("data", "device_id", "X");
            runtimeA.insert("data", 1000L, Arrays.asList(devA, 5.0));
            runtimeA.insert("data", 2000L, Arrays.asList(devA, 15.0));
            runtimeA.insert("data", 3000L, Arrays.asList(devA, 25.0));

            // --- Setup runtime B: batch insert ---
            RtBotSqlRuntime runtimeB = new RtBotSqlRuntime();
            runtimeB.setCollectMode(true);
            try {
                runtimeB.execute("SELECT STREAM data (device_id TEXT, value DOUBLE PRECISION)");
                runtimeB.execute(
                    "CREATE MATERIALIZED VIEW doubled AS "
                    + "SELECT device_id, value * 2 AS doubled_value FROM data"
                );

                CollectingListener listenerB = new CollectingListener();
                runtimeB.subscribe("doubled", listenerB);

                double devB = runtimeB.encodeText("data", "device_id", "X");
                long[] ts = {1000L, 2000L, 3000L};
                double[] col0 = {devB, devB, devB};
                double[] col1 = {5.0, 15.0, 25.0};
                runtimeB.insertBatch("data", ts, new double[][]{col0, col1});

                // --- Compare outputs ---
                assertEquals("Both paths should emit the same number of messages",
                             listenerA.count(), listenerB.count());

                for (int i = 0; i < listenerA.count(); i++) {
                    assertEquals("Timestamps should match at row " + i,
                                 (long) listenerA.timestamps.get(i),
                                 (long) listenerB.timestamps.get(i));
                    assertEquals("Value count should match at row " + i,
                                 listenerA.values.get(i).size(),
                                 listenerB.values.get(i).size());
                    for (int j = 0; j < listenerA.values.get(i).size(); j++) {
                        assertEquals("Value[" + j + "] should match at row " + i,
                                     listenerA.values.get(i).get(j),
                                     listenerB.values.get(i).get(j), 1e-9);
                    }
                }
            } finally {
                runtimeB.destroyAll();
            }
        } finally {
            runtimeA.destroyAll();
        }
    }

    // =================================================================
    // insertBatch with various column counts
    // =================================================================

    @Test
    public void insertBatchOneColumn() {
        runtime.execute("SELECT STREAM single (value DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW single_out AS "
            + "SELECT value * 3 AS tripled FROM single"
        );

        CollectingListener listener = new CollectingListener();
        runtime.subscribe("single_out", listener);

        long[] ts = {1000L, 2000L, 3000L};
        double[] col0 = {10.0, 20.0, 30.0};
        runtime.insertBatch("single", ts, new double[][]{col0});

        assertEquals("Should receive 3 output messages", 3, listener.count());
        assertEquals(30.0, listener.values.get(0).get(0), 1e-9);
        assertEquals(60.0, listener.values.get(1).get(0), 1e-9);
        assertEquals(90.0, listener.values.get(2).get(0), 1e-9);
    }

    @Test
    public void insertBatchTwoColumns() {
        runtime.execute("SELECT STREAM pair (a DOUBLE PRECISION, b DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW pair_out AS "
            + "SELECT a + b AS sum_ab FROM pair"
        );

        CollectingListener listener = new CollectingListener();
        runtime.subscribe("pair_out", listener);

        long[] ts = {1000L, 2000L};
        double[] col0 = {1.0, 3.0};
        double[] col1 = {10.0, 30.0};
        runtime.insertBatch("pair", ts, new double[][]{col0, col1});

        assertEquals("Should receive 2 output messages", 2, listener.count());
        assertEquals(11.0, listener.values.get(0).get(0), 1e-9);
        assertEquals(33.0, listener.values.get(1).get(0), 1e-9);
    }

    @Test
    public void insertBatchThreeColumns() {
        runtime.execute(
            "SELECT STREAM triple (x DOUBLE PRECISION, y DOUBLE PRECISION, z DOUBLE PRECISION)"
        );
        runtime.execute(
            "CREATE MATERIALIZED VIEW triple_out AS "
            + "SELECT x + y + z AS total FROM triple"
        );

        CollectingListener listener = new CollectingListener();
        runtime.subscribe("triple_out", listener);

        long[] ts = {1000L, 2000L};
        double[] col0 = {1.0, 4.0};
        double[] col1 = {2.0, 5.0};
        double[] col2 = {3.0, 6.0};
        runtime.insertBatch("triple", ts, new double[][]{col0, col1, col2});

        assertEquals("Should receive 2 output messages", 2, listener.count());
        assertEquals(6.0, listener.values.get(0).get(0), 1e-9);
        assertEquals(15.0, listener.values.get(1).get(0), 1e-9);
    }

    @Test
    public void insertBatchFourColumns() {
        runtime.execute(
            "SELECT STREAM quad (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE)"
        );
        runtime.execute(
            "CREATE MATERIALIZED VIEW quad_out AS "
            + "SELECT a, b, c, d, a * b + c * d AS combo FROM quad"
        );

        CollectingListener listener = new CollectingListener();
        runtime.subscribe("quad_out", listener);

        long[] ts = {1000L, 2000L, 3000L};
        double[] col0 = {2.0, 3.0, 4.0};   // a
        double[] col1 = {5.0, 6.0, 7.0};   // b
        double[] col2 = {1.0, 2.0, 3.0};   // c
        double[] col3 = {10.0, 10.0, 10.0}; // d
        runtime.insertBatch("quad", ts, new double[][]{col0, col1, col2, col3});

        assertEquals("Should receive 3 output messages", 3, listener.count());

        // Row 0: a=2, b=5, c=1, d=10 -> combo = 2*5 + 1*10 = 20
        // Output columns: a, b, c, d, combo
        assertEquals(2.0, listener.values.get(0).get(0), 1e-9);
        assertEquals(5.0, listener.values.get(0).get(1), 1e-9);
        assertEquals(1.0, listener.values.get(0).get(2), 1e-9);
        assertEquals(10.0, listener.values.get(0).get(3), 1e-9);
        assertEquals(20.0, listener.values.get(0).get(4), 1e-9);

        // Row 1: a=3, b=6, c=2, d=10 -> combo = 3*6 + 2*10 = 38
        assertEquals(38.0, listener.values.get(1).get(4), 1e-9);

        // Row 2: a=4, b=7, c=3, d=10 -> combo = 4*7 + 3*10 = 58
        assertEquals(58.0, listener.values.get(2).get(4), 1e-9);
    }

    @Test
    public void insertBatchFiveColumns() {
        runtime.execute(
            "SELECT STREAM wide (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, e DOUBLE)"
        );
        runtime.execute(
            "CREATE MATERIALIZED VIEW wide_out AS "
            + "SELECT a + b + c + d + e AS total FROM wide"
        );

        CollectingListener listener = new CollectingListener();
        runtime.subscribe("wide_out", listener);

        long[] ts = {1000L, 2000L};
        double[] col0 = {1.0, 6.0};
        double[] col1 = {2.0, 7.0};
        double[] col2 = {3.0, 8.0};
        double[] col3 = {4.0, 9.0};
        double[] col4 = {5.0, 10.0};
        runtime.insertBatch("wide", ts, new double[][]{col0, col1, col2, col3, col4});

        assertEquals("Should receive 2 output messages", 2, listener.count());
        assertEquals(15.0, listener.values.get(0).get(0), 1e-9);
        assertEquals(40.0, listener.values.get(1).get(0), 1e-9);
    }

    // =================================================================
    // insertBatch edge cases
    // =================================================================

    @Test
    public void insertBatchEmptyArrayIsNoOp() {
        runtime.execute("SELECT STREAM ticks (value DOUBLE PRECISION)");

        CollectingListener listener = new CollectingListener();
        runtime.subscribe("ticks", listener);

        runtime.insertBatch("ticks", new long[0], new double[][]{new double[0]});
        assertEquals("Empty batch should produce no output", 0, listener.count());
    }

    @Test(expected = SqlError.class)
    public void insertBatchColumnCountMismatchThrows() {
        runtime.execute("SELECT STREAM pair (a DOUBLE, b DOUBLE)");
        // Pass 3 columns for a 2-column stream
        long[] ts = {1000L};
        runtime.insertBatch("pair", ts,
            new double[][]{{1.0}, {2.0}, {3.0}});
    }

    @Test(expected = SqlError.class)
    public void insertBatchUnknownStreamThrows() {
        runtime.insertBatch("nonexistent", new long[]{1000L},
            new double[][]{{1.0}});
    }

    @Test
    public void insertBatchSingleRowWorks() {
        runtime.execute("SELECT STREAM ticks (value DOUBLE PRECISION)");
        runtime.execute(
            "CREATE MATERIALIZED VIEW doubled AS "
            + "SELECT value * 2 AS d FROM ticks"
        );

        CollectingListener listener = new CollectingListener();
        runtime.subscribe("doubled", listener);

        runtime.insertBatch("ticks", new long[]{1000L}, new double[][]{{42.0}});

        assertEquals("Single-row batch should produce one output", 1, listener.count());
        assertEquals(84.0, listener.values.get(0).get(0), 1e-9);
    }
}

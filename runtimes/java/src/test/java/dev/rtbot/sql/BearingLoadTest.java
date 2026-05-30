package dev.rtbot.sql;

import org.junit.Test;
import static org.junit.Assert.*;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Load test for the Java runtime using the rotating-machinery preset.
 *
 * <p>Mirrors the Python memory_profile_test.py setup exactly so that
 * throughput numbers are directly comparable across runtimes.
 *
 * <p>Pipeline:
 * <pre>
 *   vibration_raw  ->  vibration_moments (VIEW)
 *                          |-> rms_trend       (MATERIALIZED)
 *                          |-> kurtosis_trend  (MATERIALIZED)
 * </pre>
 *
 * <p>Usage: {@code bazel test //runtimes/java:bearing-load-test --test_output=all}
 */
public class BearingLoadTest {

    // -- Configuration (matches Python memory_profile_test.py) -------------

    private static final int NUM_BEARINGS =
        Integer.getInteger("rtbot.benchmark.bearings", 4);
    private static final int SAMPLES_PER_BURST =
        Integer.getInteger("rtbot.benchmark.samplesPerBurst", 512);
    private static final int NUM_BURSTS =
        Integer.getInteger("rtbot.benchmark.bursts", 300);
    private static final int SAMPLE_INTERVAL = 10;

    // -- SQL Definitions (matching Python test exactly) --------------------

    private static final String SQL_STREAM =
        "SELECT STREAM vibration_raw ("
        + "device_id DOUBLE, channel_id DOUBLE, amplitude DOUBLE)";

    private static final String SQL_BASE_VIEW =
        "CREATE VIEW vibration_moments AS "
        + "SELECT device_id, channel_id, "
        + "AVG(amplitude) AS mean_value, "
        + "AVG(POWER(amplitude, 2)) AS ex2, "
        + "AVG(POWER(amplitude, 3)) AS ex3, "
        + "AVG(POWER(amplitude, 4)) AS ex4, "
        + "MAX(ABS(amplitude)) AS peak_value, "
        + "AVG(ABS(amplitude)) AS mean_abs, "
        + "AVG(POWER(ABS(amplitude), 0.5)) AS mean_sqrt_abs, "
        + "COUNT(*) AS sample_count "
        + "FROM vibration_raw "
        + "GROUP BY device_id, channel_id, ABS(amplitude) > 0";

    private static final String SQL_RMS =
        "CREATE MATERIALIZED VIEW rms_trend AS "
        + "SELECT device_id, channel_id, "
        + "POWER(ex2, 0.5) AS rms "
        + "FROM vibration_moments "
        + "WHERE sample_count > 1";

    private static final String SQL_KURTOSIS =
        "CREATE MATERIALIZED VIEW kurtosis_trend AS "
        + "SELECT device_id, channel_id, "
        + "ex2 - mean_value * mean_value AS variance, "
        + "POWER(ex2 - mean_value * mean_value, 0.5) AS std_dev, "
        + "ex4 - 4.0 * ex3 * mean_value "
        + "+ 6.0 * ex2 * mean_value * mean_value "
        + "- 3.0 * mean_value * mean_value * mean_value * mean_value AS m4, "
        + "(ex4 - 4.0 * ex3 * mean_value "
        + "+ 6.0 * ex2 * mean_value * mean_value "
        + "- 3.0 * mean_value * mean_value * mean_value * mean_value) "
        + "/ POWER(ex2 - mean_value * mean_value, 2) AS kurtosis "
        + "FROM vibration_moments "
        + "WHERE sample_count > 1";

    // -- Signal generator (matches Python _generate_burst exactly) ---------

    /**
     * Generate a synthetic vibration burst: N data samples + 1 sentinel.
     * Same deterministic pattern as the Python test.
     */
    private static double[] generateBurstAmplitudes(int numSamples) {
        double[] amps = new double[numSamples];
        for (int i = 0; i < numSamples; i++) {
            amps[i] = 0.5 + 0.3 * ((i % 7) < 4 ? 1.0 : -1.0);
        }
        return amps;
    }

    // -- Load Test ---------------------------------------------------------

    @Test
    public void bearingPresetThroughput() {
        RtBotSqlRuntime runtime = new RtBotSqlRuntime();
        // Default: subscription mode — no data accumulated in memory

        try {
            // Consolidated session is the only path now; the
            // rtbot.benchmark.session flag is retained as a no-op for
            // benchmark-invocation compatibility.

            // -- Setup pipeline -------------------------------------------
            runtime.execute(SQL_STREAM);
            runtime.execute(SQL_BASE_VIEW);
            runtime.execute(SQL_RMS);
            runtime.execute(SQL_KURTOSIS);

            // Subscribe to outputs to count received messages
            boolean subscribeRaw = Boolean.parseBoolean(
                System.getProperty("rtbot.benchmark.subscribeRaw", "false"));
            boolean profile = Boolean.parseBoolean(
                System.getProperty("rtbot.benchmark.profile", "false"));
            boolean batch = Boolean.parseBoolean(
                System.getProperty("rtbot.benchmark.batch", "false"));
            boolean buffer = Boolean.parseBoolean(
                System.getProperty("rtbot.benchmark.buffer", "false"));
            AtomicLong rmsCount = new AtomicLong(0);
            AtomicLong kurtosisCount = new AtomicLong(0);
            AtomicLong rawCount = new AtomicLong(0);

            runtime.subscribe("rms_trend", (name, ts2, vals) -> rmsCount.incrementAndGet());
            runtime.subscribe("kurtosis_trend", (name, ts2, vals) -> kurtosisCount.incrementAndGet());
            if (subscribeRaw) {
                runtime.subscribe("vibration_raw", (name, ts2, vals) -> rawCount.incrementAndGet());
            }
            if (profile) {
                runtime.resetPerfStats();
            }

            ThreadMXBean threadMX = ManagementFactory.getThreadMXBean();
            Runtime rt = Runtime.getRuntime();

            System.gc();
            Thread.sleep(100);

            long heapBefore = rt.totalMemory() - rt.freeMemory();
            long cpuBefore = threadMX.getCurrentThreadCpuTime();
            long wallStart = System.nanoTime();

            // -- Ingestion (matches Python loop exactly) ------------------
            long ts = 1_000_000_000_000L;
            long totalMessages = 0;

            System.out.println();
            System.out.println("================================================================");
            System.out.println("BEARING PRESET LOAD TEST - Java Runtime (JNI)");
            System.out.println("================================================================");
            System.out.printf("Config: %d bearings x %d samples/burst x %d bursts%n",
                              NUM_BEARINGS, SAMPLES_PER_BURST, NUM_BURSTS);
            System.out.printf("Pipeline: vibration_raw -> vibration_moments (VIEW)%n");
            System.out.printf("          -> rms_trend + kurtosis_trend (MAT VIEWs)%n");
            System.out.println("Mode: SUBSCRIPTION (nothing stored, output to subscribers only)");
            System.out.printf("Feed mode: %s%n",
                    buffer ? "BUFFER insertBuffer"
                           : (batch ? "BATCH insertBatch" : "ROW insert3"));
            System.out.println("----------------------------------------------------------------");
            System.out.printf("  %6s  %10s  %12s  %10s%n",
                              "Burst", "Msgs", "Msgs/sec", "Heap MB");
            System.out.printf("  %6s  %10s  %12s  %10s%n",
                              "------", "----------", "------------", "----------");

            long lastSampleWall = wallStart;
            long lastSampleMessages = 0;

            for (int burstIdx = 0; burstIdx < NUM_BURSTS; burstIdx++) {
                for (int bearingId = 1; bearingId <= NUM_BEARINGS; bearingId++) {
                    double[] amps = generateBurstAmplitudes(SAMPLES_PER_BURST);
                    if (batch || buffer) {
                        int batchSize = SAMPLES_PER_BURST + 1;
                        long[] tsBatch = new long[batchSize];
                        double[] v0Batch = new double[batchSize];
                        double[] v1Batch = new double[batchSize];
                        double[] v2Batch = new double[batchSize];
                        for (int i = 0; i < SAMPLES_PER_BURST; i++) {
                            tsBatch[i] = ts + i;
                            v0Batch[i] = (double) bearingId;
                            v1Batch[i] = 1.0;
                            v2Batch[i] = amps[i];
                        }
                        tsBatch[SAMPLES_PER_BURST] = ts + SAMPLES_PER_BURST;
                        v0Batch[SAMPLES_PER_BURST] = (double) bearingId;
                        v1Batch[SAMPLES_PER_BURST] = 1.0;
                        v2Batch[SAMPLES_PER_BURST] = 0.0;
                        if (buffer) {
                            runtime.insertBuffer("vibration_raw", tsBatch,
                                    new double[][] { v0Batch, v1Batch, v2Batch });
                        } else {
                            runtime.insertBatch("vibration_raw", tsBatch,
                                    new double[][] { v0Batch, v1Batch, v2Batch });
                        }
                        totalMessages += batchSize;
                    } else {
                        // Data samples
                        for (int i = 0; i < amps.length; i++) {
                            runtime.insert3("vibration_raw", ts + i,
                                (double) bearingId, 1.0, amps[i]);
                            totalMessages++;
                        }
                        // Sentinel (amplitude = 0 triggers GROUP BY emission)
                        runtime.insert3("vibration_raw", ts + SAMPLES_PER_BURST,
                            (double) bearingId, 1.0, 0.0);
                        totalMessages++;
                    }

                    // Advance timestamp same as Python: base_ts += samples_per_burst + 100
                    ts += SAMPLES_PER_BURST + 100;
                }

                // Interim report
                if ((burstIdx + 1) % SAMPLE_INTERVAL == 0) {
                    long nowWall = System.nanoTime();
                    double intervalSec = (nowWall - lastSampleWall) / 1e9;
                    long intervalMsgs = totalMessages - lastSampleMessages;
                    double intervalRate = intervalMsgs / intervalSec;
                    long heapNow = rt.totalMemory() - rt.freeMemory();

                    System.out.printf("  %6d  %,10d  %,12.0f  %10.1f%n",
                                      burstIdx + 1, totalMessages, intervalRate,
                                      heapNow / (1024.0 * 1024.0));

                    lastSampleWall = nowWall;
                    lastSampleMessages = totalMessages;
                }
            }

            // -- Final measurements ---------------------------------------
            long wallEnd = System.nanoTime();
            long cpuAfter = threadMX.getCurrentThreadCpuTime();
            long heapAfter = rt.totalMemory() - rt.freeMemory();

            double wallSeconds = (wallEnd - wallStart) / 1e9;
            double cpuSeconds = (cpuAfter - cpuBefore) / 1e9;
            double cpuUtilization = (cpuSeconds / wallSeconds) * 100.0;
            double msgsPerSec = totalMessages / wallSeconds;

            int expectedPerMv = NUM_BURSTS * NUM_BEARINGS;

            // -- Report ---------------------------------------------------
            System.out.println("================================================================");
            System.out.println("RESULTS");
            System.out.println("================================================================");
            System.out.println();
            System.out.println("Throughput:");
            System.out.printf("  Messages/sec:     %,.0f%n", msgsPerSec);
            System.out.printf("  Total messages:   %,d%n", totalMessages);
            System.out.printf("  Total bursts:     %d x %d bearings = %d pipeline evals%n",
                              NUM_BURSTS, NUM_BEARINGS, NUM_BURSTS * NUM_BEARINGS);
            System.out.println();
            System.out.println("Timing:");
            System.out.printf("  Wall clock:       %.2f s%n", wallSeconds);
            System.out.printf("  CPU time:         %.2f s%n", cpuSeconds);
            System.out.printf("  CPU utilization:  %.1f%%%n", cpuUtilization);
            System.out.println();
            System.out.println("Memory:");
            System.out.printf("  Heap before:      %.1f MB%n", heapBefore / (1024.0 * 1024.0));
            System.out.printf("  Heap after:       %.1f MB%n", heapAfter / (1024.0 * 1024.0));
            System.out.printf("  Heap delta:       %.1f MB%n",
                              (heapAfter - heapBefore) / (1024.0 * 1024.0));
            System.out.println();
            System.out.println("Subscriber counts:");
            System.out.printf("  %-30s: %,8d messages%n",
                              "vibration_raw subscriber", rawCount.get());
            System.out.printf("  %-30s: %,8d messages (expected %d)%n",
                              "rms_trend subscriber", rmsCount.get(), expectedPerMv);
            System.out.printf("  %-30s: %,8d messages (expected %d)%n",
                              "kurtosis_trend subscriber", kurtosisCount.get(), expectedPerMv);
            System.out.println();

            if (profile) {
                Map<String, Long> perf = runtime.getPerfStats();
                long runnerFeedCalls = perf.getOrDefault("runner_feed_calls", 0L);
                long runnerMarshalNs = perf.getOrDefault("runner_marshal_ns", 0L);
                long runnerNativeCallNs = perf.getOrDefault("runner_native_call_ns", 0L);
                long runnerJsonParseNs = perf.getOrDefault("runner_json_parse_ns", 0L);
                long nativeTotalNs = perf.getOrDefault("native_total_ns", 0L);
                long nativeReceiveNs = perf.getOrDefault("native_receive_ns", 0L);
                long nativeJsonNs = perf.getOrDefault("native_json_serialize_ns", 0L);
                long nativeCopyNs = perf.getOrDefault("native_values_copy_ns", 0L);
                long nativeJstringNs = perf.getOrDefault("native_jstring_ns", 0L);
                long boundaryNs = Math.max(0L, runnerNativeCallNs - nativeTotalNs);

                double totalWallNs = wallSeconds * 1e9;
                System.out.println("Feed-path timing breakdown (cumulative):");
                System.out.printf("  %-34s %8d%n", "runner feed() calls", runnerFeedCalls);
                if (runnerFeedCalls > 0) {
                    System.out.printf("  %-34s %8.1f ns/call%n", "runner marshal",
                                      (double) runnerMarshalNs / runnerFeedCalls);
                    System.out.printf("  %-34s %8.1f ns/call%n", "runner native call",
                                      (double) runnerNativeCallNs / runnerFeedCalls);
                    System.out.printf("  %-34s %8.1f ns/call%n", "runner JSON parse",
                                      (double) runnerJsonParseNs / runnerFeedCalls);
                    System.out.printf("  %-34s %8.1f ns/call%n", "estimated JNI boundary",
                                      (double) boundaryNs / runnerFeedCalls);
                }
                System.out.printf("  %-34s %8.1f%%%n", "runner marshal share",
                                  totalWallNs > 0 ? (runnerMarshalNs * 100.0 / totalWallNs) : 0.0);
                System.out.printf("  %-34s %8.1f%%%n", "runner native-call share",
                                  totalWallNs > 0 ? (runnerNativeCallNs * 100.0 / totalWallNs) : 0.0);
                System.out.printf("  %-34s %8.1f%%%n", "runner parse share",
                                  totalWallNs > 0 ? (runnerJsonParseNs * 100.0 / totalWallNs) : 0.0);
                System.out.println();
                System.out.println("Native feedPipeline timing (inside JNI):");
                System.out.printf("  %-34s %8d%n", "native feed calls",
                                  perf.getOrDefault("native_feed_calls", 0L));
                if (runnerFeedCalls > 0) {
                    System.out.printf("  %-34s %8.1f ns/call%n", "native total",
                                      (double) nativeTotalNs / runnerFeedCalls);
                    System.out.printf("  %-34s %8.1f ns/call%n", "native values copy",
                                      (double) nativeCopyNs / runnerFeedCalls);
                    System.out.printf("  %-34s %8.1f ns/call%n", "native receive",
                                      (double) nativeReceiveNs / runnerFeedCalls);
                    System.out.printf("  %-34s %8.1f ns/call%n", "native batch->json",
                                      (double) nativeJsonNs / runnerFeedCalls);
                    System.out.printf("  %-34s %8.1f ns/call%n", "native jstring",
                                      (double) nativeJstringNs / runnerFeedCalls);
                }
                System.out.println();
            }

            // -- IMS projection -------------------------------------------
            int imsBursts = 2156;
            int imsSamples = 20480;
            int imsBearings = 4;
            long imsTotal = (long) imsBursts * imsBearings * (imsSamples + 1);
            double imsEstSec = imsTotal / msgsPerSec;
            System.out.println("IMS Dataset 1 projection (2156 bursts x 4 bearings x 20480 samples):");
            System.out.printf("  Total messages:   %,d%n", imsTotal);
            System.out.printf("  Estimated time:   %.1f s (%.1f min)%n",
                              imsEstSec, imsEstSec / 60);
            System.out.println("================================================================");

            // -- Assertions -----------------------------------------------
            // Subscribers should have received all messages
            if (subscribeRaw) {
                assertEquals("Raw subscriber should receive all messages",
                             totalMessages, rawCount.get());
            }
            assertEquals("RMS subscriber should receive expected count",
                         expectedPerMv, (int) rmsCount.get());
            assertEquals("Kurtosis subscriber should receive expected count",
                         expectedPerMv, (int) kurtosisCount.get());
            assertTrue("Should sustain > 1000 msgs/sec", msgsPerSec > 1000);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted: " + e.getMessage());
        } finally {
            runtime.destroyAll();
        }
    }

    // -- Subscription Mode Load Test (default) ----------------------------

    /**
     * Validates that the default subscription model keeps memory stable by
     * not accumulating any data. Subscribers receive output in real time
     * while the in-memory store remains empty.
     *
     * <p>This is the production-oriented mode (and the default). When no
     * subscriber is listening, data is discarded — never accumulated.
     * <ul>
     *   <li>Raw stream data is NOT stored (0 rows in store)</li>
     *   <li>Materialized view output is NOT stored (0 rows in store)</li>
     *   <li>Subscribers receive all output messages in real time</li>
     *   <li>Memory delta is minimal compared to collect mode</li>
     * </ul>
     */
    @Test
    public void bearingPresetSubscriptionMode() {
        RtBotSqlRuntime runtime = new RtBotSqlRuntime();
        // Default: collect mode off — subscription-based output

        try {
            // -- Setup pipeline -------------------------------------------
            runtime.execute(SQL_STREAM);
            runtime.execute(SQL_BASE_VIEW);
            runtime.execute(SQL_RMS);
            runtime.execute(SQL_KURTOSIS);

            // Subscribe to output views to count received messages
            AtomicLong rmsCount = new AtomicLong(0);
            AtomicLong kurtosisCount = new AtomicLong(0);
            AtomicLong rawCount = new AtomicLong(0);

            runtime.subscribe("rms_trend", (name, ts, vals) -> rmsCount.incrementAndGet());
            runtime.subscribe("kurtosis_trend", (name, ts, vals) -> kurtosisCount.incrementAndGet());
            runtime.subscribe("vibration_raw", (name, ts, vals) -> rawCount.incrementAndGet());

            Runtime rt = Runtime.getRuntime();
            System.gc();
            Thread.sleep(100);
            long heapBefore = rt.totalMemory() - rt.freeMemory();
            long wallStart = System.nanoTime();

            // -- Ingestion ------------------------------------------------
            long ts = 1_000_000_000_000L;
            long totalMessages = 0;

            System.out.println();
            System.out.println("================================================================");
            System.out.println("BEARING PRESET SUBSCRIPTION MODE TEST - Java Runtime (JNI)");
            System.out.println("================================================================");
            System.out.printf("Config: %d bearings x %d samples/burst x %d bursts%n",
                              NUM_BEARINGS, SAMPLES_PER_BURST, NUM_BURSTS);
            System.out.println("Mode: SUBSCRIPTION (nothing stored, output to subscribers only)");
            System.out.println("----------------------------------------------------------------");

            for (int burstIdx = 0; burstIdx < NUM_BURSTS; burstIdx++) {
                for (int bearingId = 1; bearingId <= NUM_BEARINGS; bearingId++) {
                    double[] amps = generateBurstAmplitudes(SAMPLES_PER_BURST);

                    for (int i = 0; i < amps.length; i++) {
                        runtime.insert("vibration_raw", ts + i,
                            Arrays.asList((double) bearingId, 1.0, amps[i]));
                        totalMessages++;
                    }
                    runtime.insert("vibration_raw", ts + SAMPLES_PER_BURST,
                        Arrays.asList((double) bearingId, 1.0, 0.0));
                    totalMessages++;

                    ts += SAMPLES_PER_BURST + 100;
                }
            }

            // -- Final measurements ---------------------------------------
            long wallEnd = System.nanoTime();
            long heapAfter = rt.totalMemory() - rt.freeMemory();

            double wallSeconds = (wallEnd - wallStart) / 1e9;
            double msgsPerSec = totalMessages / wallSeconds;

            // -- Store stats (subscription mode) ---------------------------
            int rmsRows = runtime.getStore().read("rms_trend").size();
            int kurtosisRows = runtime.getStore().read("kurtosis_trend").size();
            int rawRows = runtime.getStore().read("vibration_raw").size();
            int viewRows = runtime.getStore().read("vibration_moments").size();

            int expectedPerMv = NUM_BURSTS * NUM_BEARINGS;

            // -- Report ---------------------------------------------------
            System.out.println("================================================================");
            System.out.println("SUBSCRIPTION MODE RESULTS");
            System.out.println("================================================================");
            System.out.println();
            System.out.println("Throughput:");
            System.out.printf("  Messages/sec:     %,.0f%n", msgsPerSec);
            System.out.printf("  Total messages:   %,d%n", totalMessages);
            System.out.println();
            System.out.println("Memory:");
            System.out.printf("  Heap before:      %.1f MB%n", heapBefore / (1024.0 * 1024.0));
            System.out.printf("  Heap after:       %.1f MB%n", heapAfter / (1024.0 * 1024.0));
            System.out.printf("  Heap delta:       %.1f MB%n",
                              (heapAfter - heapBefore) / (1024.0 * 1024.0));
            System.out.println();
            System.out.println("Store contents (all should be 0 in subscription mode):");
            System.out.printf("  %-30s: %,8d rows (should be 0)%n",
                              "vibration_raw (raw)", rawRows);
            System.out.printf("  %-30s: %,8d rows (should be 0)%n",
                              "vibration_moments (VIEW)", viewRows);
            System.out.printf("  %-30s: %,8d rows (should be 0)%n",
                              "rms_trend (MAT VIEW)", rmsRows);
            System.out.printf("  %-30s: %,8d rows (should be 0)%n",
                              "kurtosis_trend (MAT VIEW)", kurtosisRows);
            System.out.println();
            System.out.println("Subscriber counts:");
            System.out.printf("  %-30s: %,8d messages%n",
                              "vibration_raw subscriber", rawCount.get());
            System.out.printf("  %-30s: %,8d messages (expected %d)%n",
                              "rms_trend subscriber", rmsCount.get(), expectedPerMv);
            System.out.printf("  %-30s: %,8d messages (expected %d)%n",
                              "kurtosis_trend subscriber", kurtosisCount.get(), expectedPerMv);
            System.out.println("================================================================");

            // -- Assertions -----------------------------------------------
            // In subscription mode, nothing is stored
            assertEquals("Raw stream should NOT be stored", 0, rawRows);
            assertEquals("Plain VIEW should not store output", 0, viewRows);
            assertEquals("rms_trend should NOT be stored", 0, rmsRows);
            assertEquals("kurtosis_trend should NOT be stored", 0, kurtosisRows);

            // Subscribers should have received all messages
            assertEquals("Raw subscriber should receive all messages",
                         totalMessages, rawCount.get());
            assertEquals("RMS subscriber should receive expected count",
                         expectedPerMv, (int) rmsCount.get());
            assertEquals("Kurtosis subscriber should receive expected count",
                         expectedPerMv, (int) kurtosisCount.get());

            assertTrue("Should sustain > 1000 msgs/sec", msgsPerSec > 1000);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted: " + e.getMessage());
        } finally {
            runtime.destroyAll();
        }
    }
}

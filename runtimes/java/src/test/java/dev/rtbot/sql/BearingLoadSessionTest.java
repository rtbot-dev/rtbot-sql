package dev.rtbot.sql;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.junit.Test;
import static org.junit.Assert.*;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consolidated-session variant of {@link BearingLoadTest}. Same preset
 * pipeline (vibration_raw → vibration_moments → {rms_trend, kurtosis_trend})
 * but the three views are merged into a single rtbot Program via
 * {@link RtBotSqlCompiler#compileSessionJson(String)}. Data is fed to the
 * single session handle; outputs are demuxed to per-view subscribers using
 * the view_terminals map returned by the compiler.
 *
 * <p>Run:
 * <pre>
 *   bazel test //runtimes/java:bearing-load-session-test \
 *     --test_output=all \
 *     --jvmopt=-Drtbot.benchmark.bearings=1 \
 *     --jvmopt=-Drtbot.benchmark.buffer=true
 * </pre>
 */
public class BearingLoadSessionTest {

    // -- Configuration (matches BearingLoadTest exactly) -------------------

    private static final int NUM_BEARINGS =
        Integer.getInteger("rtbot.benchmark.bearings", 4);
    private static final int SAMPLES_PER_BURST =
        Integer.getInteger("rtbot.benchmark.samplesPerBurst", 512);
    private static final int NUM_BURSTS =
        Integer.getInteger("rtbot.benchmark.bursts", 300);
    private static final int SAMPLE_INTERVAL = 10;

    private static final String SQL_STREAM =
        "CREATE STREAM vibration_raw ("
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

    private static double[] generateBurstAmplitudes(int numSamples) {
        double[] amps = new double[numSamples];
        for (int i = 0; i < numSamples; i++) {
            amps[i] = 0.5 + 0.3 * ((i % 7) < 4 ? 1.0 : -1.0);
        }
        return amps;
    }

    @Test
    public void bearingPresetThroughputSession() throws Exception {
        // Use RtBotSqlRuntime only to populate the catalog with per-view
        // program_jsons (side-effect of handleCreateView). The deployed
        // per-view pipelines are unused in this test — all data feeds the
        // consolidated session pipeline below. destroyAll() at the end
        // reclaims them.
        RtBotSqlRuntime runtime = new RtBotSqlRuntime();
        long sessionHandle = 0L;
        try {
            runtime.execute(SQL_STREAM);
            runtime.execute(SQL_BASE_VIEW);
            runtime.execute(SQL_RMS);
            runtime.execute(SQL_KURTOSIS);

            // Compile the consolidated session.
            String catalogJson = runtime.getCatalog().snapshotJson();
            String sessionJson = RtBotSqlCompiler.compileSessionJson(catalogJson);
            Gson gson = new Gson();
            JsonObject sessionObj = gson.fromJson(sessionJson, JsonObject.class);
            JsonArray errs = sessionObj.getAsJsonArray("errors");
            if (errs != null && errs.size() > 0) {
                fail("compile_session_program failed: " + errs.toString());
            }
            String programJson = sessionObj.get("program_json").getAsString();

            // Build terminal-operator-id → view-name map for output demux.
            Map<String, String> terminalToView = new HashMap<>();
            JsonObject vt = sessionObj.getAsJsonObject("view_terminals");
            for (Map.Entry<String, JsonElement> e : vt.entrySet()) {
                terminalToView.put(e.getValue().getAsString(), e.getKey());
            }

            sessionHandle = RtBotSqlCompiler.createPipeline(programJson);

            boolean profile = Boolean.parseBoolean(
                System.getProperty("rtbot.benchmark.profile", "false"));
            boolean batch = Boolean.parseBoolean(
                System.getProperty("rtbot.benchmark.batch", "false"));
            boolean buffer = Boolean.parseBoolean(
                System.getProperty("rtbot.benchmark.buffer", "false"));
            AtomicLong rmsCount = new AtomicLong(0);
            AtomicLong kurtosisCount = new AtomicLong(0);

            if (profile) {
                RtBotSqlCompiler.setNativeFeedStatsEnabled(true);
                RtBotSqlCompiler.resetNativeFeedStats();
            }

            ThreadMXBean threadMX = ManagementFactory.getThreadMXBean();
            Runtime rt = Runtime.getRuntime();

            System.gc();
            Thread.sleep(100);

            long heapBefore = rt.totalMemory() - rt.freeMemory();
            long cpuBefore = threadMX.getCurrentThreadCpuTime();
            long wallStart = System.nanoTime();

            long ts = 1_000_000_000_000L;
            long totalMessages = 0;
            long jniCalls = 0;

            System.out.println();
            System.out.println("================================================================");
            System.out.println("BEARING PRESET LOAD TEST - CONSOLIDATED SESSION");
            System.out.println("================================================================");
            System.out.printf("Config: %d bearings x %d samples/burst x %d bursts%n",
                              NUM_BEARINGS, SAMPLES_PER_BURST, NUM_BURSTS);
            System.out.println("Pipeline: vibration_raw -> ONE consolidated rtbot Program");
            System.out.println("          (vibration_moments + rms_trend + kurtosis_trend inlined)");
            System.out.printf("Feed mode: %s%n",
                    buffer ? "BUFFER feedPipelineBufferI1"
                           : (batch ? "BATCH feedPipelineBatchI1" : "ROW feedPipeline3I1"));
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
                        double[][] cols = new double[][] { v0Batch, v1Batch, v2Batch };

                        String outJson = buffer
                            ? RtBotSqlCompiler.feedPipelineBufferI1(sessionHandle, tsBatch, cols)
                            : RtBotSqlCompiler.feedPipelineBatchI1(sessionHandle, tsBatch, cols);
                        jniCalls++;
                        demuxAndCount(outJson, terminalToView, rmsCount, kurtosisCount, gson);
                        totalMessages += batchSize;
                    } else {
                        for (int i = 0; i < amps.length; i++) {
                            String outJson = RtBotSqlCompiler.feedPipeline3I1(
                                sessionHandle, ts + i,
                                (double) bearingId, 1.0, amps[i]);
                            jniCalls++;
                            demuxAndCount(outJson, terminalToView, rmsCount, kurtosisCount, gson);
                            totalMessages++;
                        }
                        String outJson = RtBotSqlCompiler.feedPipeline3I1(
                            sessionHandle, ts + SAMPLES_PER_BURST,
                            (double) bearingId, 1.0, 0.0);
                        jniCalls++;
                        demuxAndCount(outJson, terminalToView, rmsCount, kurtosisCount, gson);
                        totalMessages++;
                    }

                    ts += SAMPLES_PER_BURST + 100;
                }

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

            long wallEnd = System.nanoTime();
            long cpuAfter = threadMX.getCurrentThreadCpuTime();
            long heapAfter = rt.totalMemory() - rt.freeMemory();

            double wallSeconds = (wallEnd - wallStart) / 1e9;
            double cpuSeconds = (cpuAfter - cpuBefore) / 1e9;
            double cpuUtilization = (cpuSeconds / wallSeconds) * 100.0;
            double msgsPerSec = totalMessages / wallSeconds;
            int expectedPerMv = NUM_BURSTS * NUM_BEARINGS;

            System.out.println("================================================================");
            System.out.println("RESULTS (CONSOLIDATED SESSION)");
            System.out.println("================================================================");
            System.out.println();
            System.out.println("Throughput:");
            System.out.printf("  Messages/sec:     %,.0f%n", msgsPerSec);
            System.out.printf("  Total messages:   %,d%n", totalMessages);
            System.out.printf("  JNI feed calls:   %,d%n", jniCalls);
            System.out.printf("  Msgs per JNI:     %.1f%n",
                              jniCalls > 0 ? (double) totalMessages / jniCalls : 0.0);
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
            System.out.println("Output counts (from session demux):");
            System.out.printf("  %-30s: %,8d messages (expected %d)%n",
                              "rms_trend", rmsCount.get(), expectedPerMv);
            System.out.printf("  %-30s: %,8d messages (expected %d)%n",
                              "kurtosis_trend", kurtosisCount.get(), expectedPerMv);
            System.out.println("================================================================");

            assertEquals("RMS count", expectedPerMv, (int) rmsCount.get());
            assertEquals("Kurtosis count", expectedPerMv, (int) kurtosisCount.get());
            assertTrue("Should sustain > 1000 msgs/sec", msgsPerSec > 1000);

        } finally {
            if (sessionHandle != 0L) {
                RtBotSqlCompiler.destroyPipeline(sessionHandle);
            }
            runtime.destroyAll();
        }
    }

    // Demux the session output JSON by upstream operator id, mapping back
    // to view names, and increment the per-view counters.
    private static void demuxAndCount(String outJson,
                                      Map<String, String> terminalToView,
                                      AtomicLong rmsCount,
                                      AtomicLong kurtosisCount,
                                      Gson gson) {
        if (outJson == null) return;
        JsonArray arr = gson.fromJson(outJson, JsonArray.class);
        if (arr == null) return;
        for (JsonElement el : arr) {
            JsonObject row = el.getAsJsonObject();
            String opId = row.get("operator_id").getAsString();
            String viewName = terminalToView.get(opId);
            if (viewName == null) continue;
            if ("rms_trend".equals(viewName)) rmsCount.incrementAndGet();
            else if ("kurtosis_trend".equals(viewName)) kurtosisCount.incrementAndGet();
        }
    }
}

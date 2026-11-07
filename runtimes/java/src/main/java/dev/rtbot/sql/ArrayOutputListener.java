package dev.rtbot.sql;

/**
 * Primitive fast-path variant of {@link OutputListener}.
 *
 * <p>The {@code values} array is a reusable scratch buffer; only indices
 * {@code [0, nValues)} are valid for the duration of the callback. Callers
 * that need to retain the data must copy the relevant slice.
 *
 * <p>Use {@link OutputListener#ofArray(ArrayOutputListener)} to wrap an
 * instance as a standard {@link OutputListener}.
 *
 * @see OutputListener#ofArray(ArrayOutputListener)
 */
@FunctionalInterface
public interface ArrayOutputListener {

    /**
     * Called when a new message is produced for a subscribed stream or view.
     *
     * @param streamName the name of the stream or view
     * @param timestamp  the message timestamp
     * @param values     reusable scratch array; valid range {@code [0, nValues)}
     * @param nValues    number of valid entries in {@code values}
     */
    void onMessage(String streamName, long timestamp, double[] values, int nValues);
}

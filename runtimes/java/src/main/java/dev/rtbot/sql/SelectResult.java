package dev.rtbot.sql;

import java.util.ArrayList;
import java.util.List;

/**
 * Result of a SELECT query execution.
 */
public class SelectResult {

    public List<String> columns;
    public List<List<Double>> rows;
    public List<Long> timestamps;

    public SelectResult() {
        this.columns = new ArrayList<>();
        this.rows = new ArrayList<>();
        this.timestamps = new ArrayList<>();
    }

    public SelectResult(List<String> columns, List<List<Double>> rows, List<Long> timestamps) {
        this.columns = columns != null ? new ArrayList<>(columns) : new ArrayList<>();
        this.rows = rows != null ? new ArrayList<>(rows) : new ArrayList<>();
        this.timestamps = timestamps != null ? new ArrayList<>(timestamps) : new ArrayList<>();
    }

    /**
     * Returns the number of result rows.
     */
    public int rowCount() {
        return rows.size();
    }

    @Override
    public String toString() {
        return "SelectResult{columns=" + columns + ", rowCount=" + rows.size() + '}';
    }
}

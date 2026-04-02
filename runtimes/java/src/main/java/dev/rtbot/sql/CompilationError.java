package dev.rtbot.sql;

import com.google.gson.annotations.SerializedName;

/**
 * A single compilation error from the RTBot SQL compiler.
 */
public class CompilationError {

    @SerializedName("message")
    public String message;

    @SerializedName("line")
    public int line;

    @SerializedName("column")
    public int column;

    public CompilationError() {
        this.message = "";
        this.line = -1;
        this.column = -1;
    }

    public CompilationError(String message, int line, int column) {
        this.message = message;
        this.line = line;
        this.column = column;
    }

    @Override
    public String toString() {
        return "CompilationError{message='" + message + "', line=" + line + ", column=" + column + '}';
    }
}

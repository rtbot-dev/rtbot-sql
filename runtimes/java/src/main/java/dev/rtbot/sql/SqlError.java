package dev.rtbot.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Exception thrown when SQL compilation or execution produces errors.
 */
public class SqlError extends RuntimeException {

    private final List<CompilationError> errors;

    /**
     * Construct from a list of compilation errors.
     */
    public SqlError(List<CompilationError> errors) {
        super(errors.stream()
                .map(e -> e.message)
                .collect(Collectors.joining("; ")));
        this.errors = new ArrayList<>(errors);
    }

    /**
     * Construct from a single error message.
     */
    public SqlError(String message) {
        super(message);
        CompilationError e = new CompilationError();
        e.message = message;
        e.line = -1;
        e.column = -1;
        this.errors = List.of(e);
    }

    /**
     * Returns the list of compilation errors.
     */
    public List<CompilationError> getErrors() {
        return errors;
    }
}

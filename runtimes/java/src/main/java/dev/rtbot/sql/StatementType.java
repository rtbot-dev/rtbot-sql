package dev.rtbot.sql;

/**
 * Types of SQL statements recognized by the RTBot SQL compiler.
 */
public enum StatementType {
    SELECT_STREAM,
    CREATE_VIEW,
    CREATE_MATERIALIZED_VIEW,
    CREATE_TABLE,
    INSERT,
    SELECT,
    SUBSCRIBE,
    DROP,
    DELETE
}

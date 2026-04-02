package dev.rtbot.sql;

/**
 * SELECT query execution tiers.
 *
 * <ul>
 *   <li>{@link #TIER1_READ} — direct read from materialized store</li>
 *   <li>{@link #TIER2_SCAN} — scan with filtering</li>
 *   <li>{@link #TIER3_EPHEMERAL} — ephemeral pipeline execution</li>
 * </ul>
 */
public enum SelectTier {
    TIER1_READ,
    TIER2_SCAN,
    TIER3_EPHEMERAL
}

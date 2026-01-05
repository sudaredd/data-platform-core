package com.platform.data.common.test;

/**
 * Test helper class containing CQL schema definitions for integration tests.
 * This schema represents a typical multi-tenant time-series data model with
 * UDTs.
 */
public class SchemaInit {

    /**
     * Returns the complete CQL schema for testing.
     * This includes:
     * - A UDT (numeric_data_point) for structured data storage
     * - A table (DailyNumeric) with composite partition key and bucketing by year
     */
    public static String getSchema() {
        return """
                CREATE TYPE IF NOT EXISTS numeric_data_point (
                    value decimal,
                    report_time timestamp
                );

                CREATE TABLE IF NOT EXISTS DailyNumeric (
                    tenant_id text,
                    instrument_id text,
                    period_year int,
                    period_date date,
                    field_id text,
                    data frozen<numeric_data_point>,
                    PRIMARY KEY ((tenant_id, instrument_id, period_year), period_date, field_id)
                );
                """;
    }

    /**
     * Returns the keyspace name used in tests.
     */
    public static String getKeyspace() {
        return "test_keyspace";
    }

    /**
     * Returns the table name used in tests.
     */
    public static String getTableName() {
        return "DailyNumeric";
    }

    /**
     * Returns the UDT name used in tests.
     */
    public static String getUdtName() {
        return "numeric_data_point";
    }

    /**
     * Returns the full keyspace creation CQL.
     */
    public static String getKeyspaceCreation() {
        return """
                CREATE KEYSPACE IF NOT EXISTS test_keyspace
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
                """;
    }
}

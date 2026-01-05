package com.platform.data.ingest.integration;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.platform.data.common.config.TenantConfig;
import com.platform.data.common.registry.TenantConfigRegistry;
import com.platform.data.common.test.SchemaInit;
import com.platform.data.ingest.dto.IngestBatchRequest;
import com.platform.data.ingest.service.DynamicIngestService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for DynamicIngestService with real Cassandra.
 */
@Testcontainers
class IngestServiceIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(IngestServiceIntegrationTest.class);

    @Container
    static CassandraContainer<?> cassandra = new CassandraContainer<>("cassandra:4.1")
            .withExposedPorts(9042);

    private static CqlSession session;
    private static DynamicIngestService ingestService;
    private static TenantConfigRegistry registry;

    @BeforeAll
    static void setup() {
        cassandra.start();

        session = new CqlSessionBuilder()
                .addContactPoint(new InetSocketAddress(
                        cassandra.getHost(),
                        cassandra.getMappedPort(9042)))
                .withLocalDatacenter("datacenter1")
                .build();

        log.info("Connected to Cassandra at {}:{}",
                cassandra.getHost(), cassandra.getMappedPort(9042));

        initializeSchema();

        registry = new TenantConfigRegistry();
        ingestService = new DynamicIngestService(session, registry);

        registerTenant();
    }

    @AfterEach
    void cleanup() {
        session.execute("TRUNCATE " + SchemaInit.getKeyspace() + "." + SchemaInit.getTableName());
        log.info("Cleaned up test data");
    }

    private static void initializeSchema() {
        log.info("Initializing schema...");
        session.execute(SchemaInit.getKeyspaceCreation());
        session.execute("USE " + SchemaInit.getKeyspace());

        String schema = SchemaInit.getSchema();
        String[] statements = schema.split(";");

        for (String statement : statements) {
            String trimmed = statement.trim();
            if (!trimmed.isEmpty()) {
                session.execute(trimmed);
            }
        }

        log.info("Schema initialized successfully");
    }

    private static void registerTenant() {
        TenantConfig config = TenantConfig.withBucket(
                SchemaInit.getKeyspace(),
                SchemaInit.getTableName(),
                List.of("tenant_id", "instrument_id", "period_year"),
                "period_year",
                Set.of("data"));

        registry.register("IBM", "DAILY", "NUMERIC", config);
        log.info("Registered tenant: IBM");
    }

    @Test
    void testBatchIngestion_persistsDataCorrectly() {
        log.info("=== Testing Batch Ingestion ===");

        // Create batch with 5 rows
        List<Map<String, Object>> batch = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch.add(createRow("IBM", "IBM_STOCK",
                    LocalDate.of(2024, 1, 10 + i), "revenue", 100.0 + i));
        }

        IngestBatchRequest request = new IngestBatchRequest("IBM", "DAILY", batch);

        // Ingest
        ingestService.ingestBatchAsync(request).join();

        // Verify data persisted
        var results = session.execute(
                "SELECT * FROM " + SchemaInit.getKeyspace() + "." + SchemaInit.getTableName() +
                        " WHERE tenant_id = 'IBM' AND instrument_id = 'IBM_STOCK' AND period_year = 2024")
                .all();

        assertThat(results).hasSize(5);
        log.info("Successfully verified 5 rows persisted");
    }

    @Test
    void testPartitionLevelBatching_groupsByYear() {
        log.info("=== Testing Partition-Level Batching ===");

        // Create batch with mixed years
        List<Map<String, Object>> batch = new ArrayList<>();

        // 3 rows for 2023
        for (int i = 0; i < 3; i++) {
            batch.add(createRow("IBM", "IBM_STOCK",
                    LocalDate.of(2023, 12, 10 + i), "revenue", 100.0 + i));
        }

        // 3 rows for 2024
        for (int i = 0; i < 3; i++) {
            batch.add(createRow("IBM", "IBM_STOCK",
                    LocalDate.of(2024, 1, 10 + i), "profit", 50.0 + i));
        }

        IngestBatchRequest request = new IngestBatchRequest("IBM", "DAILY", batch);

        // Ingest
        ingestService.ingestBatchAsync(request).join();

        // Verify 2023 data
        var results2023 = session.execute(
                "SELECT * FROM " + SchemaInit.getKeyspace() + "." + SchemaInit.getTableName() +
                        " WHERE tenant_id = 'IBM' AND instrument_id = 'IBM_STOCK' AND period_year = 2023")
                .all();

        // Verify 2024 data
        var results2024 = session.execute(
                "SELECT * FROM " + SchemaInit.getKeyspace() + "." + SchemaInit.getTableName() +
                        " WHERE tenant_id = 'IBM' AND instrument_id = 'IBM_STOCK' AND period_year = 2024")
                .all();

        assertThat(results2023).hasSize(3);
        assertThat(results2024).hasSize(3);
        log.info("Successfully verified partition-level batching: 3 rows in 2023, 3 rows in 2024");
    }

    private Map<String, Object> createRow(
            String tenantId,
            String instrumentId,
            LocalDate periodDate,
            String fieldId,
            double value) {
        Map<String, Object> row = new HashMap<>();
        row.put("tenant_id", tenantId);
        row.put("instrument_id", instrumentId);
        row.put("period_date", periodDate);
        row.put("field_id", fieldId);

        Map<String, Object> dataPoint = new HashMap<>();
        dataPoint.put("value", BigDecimal.valueOf(value));
        dataPoint.put("report_time", Instant.now());
        row.put("data", dataPoint);

        return row;
    }
}

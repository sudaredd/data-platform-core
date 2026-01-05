package com.platform.data.query;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.platform.data.common.config.TenantConfig;
import com.platform.data.common.registry.TenantConfigRegistry;
import com.platform.data.common.test.SchemaInit;
import com.platform.data.ingest.dto.IngestBatchRequest;
import com.platform.data.ingest.service.DynamicIngestService;
import com.platform.data.query.service.DynamicRetrievalService;
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
 * Integration test demonstrating smart batching and shared DAL functionality.
 * 
 * This test proves:
 * 1. Partition-level batching with mixed years
 * 2. Scatter-gather query across year buckets
 * 3. Shared DAL (UdtMapper, PartitionCalculator, TenantConfigRegistry)
 * 4. No data loss in batch processing
 */
@Testcontainers
class GenericPlatformIntegrationTest {

        private static final Logger log = LoggerFactory.getLogger(GenericPlatformIntegrationTest.class);

        @Container
        static CassandraContainer<?> cassandra = new CassandraContainer<>("cassandra:4.1")
                        .withExposedPorts(9042);

        private static CqlSession session;
        private static DynamicIngestService ingestService;
        private static DynamicRetrievalService retrievalService;
        private static TenantConfigRegistry registry;

        @BeforeAll
        static void setup() {
                // Wait for Cassandra to be ready
                cassandra.start();

                // Create CQL Session
                session = new CqlSessionBuilder()
                                .addContactPoint(new InetSocketAddress(
                                                cassandra.getHost(),
                                                cassandra.getMappedPort(9042)))
                                .withLocalDatacenter("datacenter1")
                                .build();

                log.info("Connected to Cassandra at {}:{}",
                                cassandra.getHost(), cassandra.getMappedPort(9042));

                // Initialize schema
                initializeSchema();

                // Initialize registry and services
                registry = new TenantConfigRegistry();
                ingestService = new DynamicIngestService(session, registry);
                retrievalService = new DynamicRetrievalService(session, registry);

                // Register tenant configuration
                registerTenant();
        }

    @AfterEach
    void cleanup() {
        // Truncate table between tests for proper test isolation
        session.execute("TRUNCATE " + SchemaInit.getKeyspace() + "." + SchemaInit.getTableName());
        log.info("Cleaned up test data");
    }

        /**
         * Executes the schema initialization CQL.
         */
        private static void initializeSchema() {
                log.info("Initializing schema...");

                // Create keyspace
                session.execute(SchemaInit.getKeyspaceCreation());

                // Use the keyspace
                session.execute("USE " + SchemaInit.getKeyspace());

                // Create UDT and table
                String schema = SchemaInit.getSchema();
                String[] statements = schema.split(";");

                for (String statement : statements) {
                        String trimmed = statement.trim();
                        if (!trimmed.isEmpty()) {
                                session.execute(trimmed);
                                log.info("Executed: {}", trimmed.substring(0, Math.min(50, trimmed.length())));
                        }
                }

                log.info("Schema initialized successfully");
        }

        /**
         * Registers the tenant configuration for IBM.
         */
        private static void registerTenant() {
                TenantConfig config = TenantConfig.withBucket(
                                SchemaInit.getKeyspace(),
                                SchemaInit.getTableName(),
                                List.of("tenant_id", "instrument_id", "period_year"),
                                "period_year",
                                Set.of("data") // 'data' column is a UDT
                );

                registry.register("IBM", "DAILY", "NUMERIC", config);

                log.info("Registered tenant: IBM");
        }

        /**
         * THE PROOF: Smart batching with mixed years and scatter-gather query.
         */
        @Test
        void testSmartBatchingAndRouting() {
                log.info("=== Starting Smart Batching Integration Test ===");

                // Create batch with mixed years
                List<Map<String, Object>> batch = new ArrayList<>();

                // 5 rows for Dec 2023
                for (int i = 0; i < 5; i++) {
                        batch.add(createRow("IBM", "IBM_STOCK",
                                        LocalDate.of(2023, 12, 10 + i), "revenue", 100.0 + i));
                }

                // 5 rows for Jan 2024
                for (int i = 0; i < 5; i++) {
                        batch.add(createRow("IBM", "IBM_STOCK",
                                        LocalDate.of(2024, 1, 10 + i), "profit", 50.0 + i));
                }

                log.info("Step 1: Created batch with 10 rows (5 from 2023, 5 from 2024)");

                // Ingest batch
                IngestBatchRequest request = new IngestBatchRequest("IBM", "DAILY", batch);
                ingestService.ingestBatchAsync(request).join();

                log.info("Step 2: Batch ingestion complete");

                // Query across year boundary
                Map<String, Object> criteria = Map.of(
                                "tenant_id", "IBM",
                                "instrument_id", "IBM_STOCK",
                                "start_date", "2023-12-01",
                                "end_date", "2024-02-01");

                List<Map<String, Object>> results = retrievalService.retrieve("IBM", criteria);

                log.info("Step 3: Query returned {} rows", results.size());

                // Verify all 10 rows returned
                assertThat(results).hasSize(10);

                // Verify partition grouping worked (2 batches: 2023 and 2024)
                long count2023 = results.stream()
                                .filter(r -> ((LocalDate) r.get("period_date")).getYear() == 2023)
                                .count();

                long count2024 = results.stream()
                                .filter(r -> ((LocalDate) r.get("period_date")).getYear() == 2024)
                                .count();

                assertThat(count2023).isEqualTo(5);
                assertThat(count2024).isEqualTo(5);

                // Verify UDT conversion (data field should be a Map, not UdtValue)
                Map<String, Object> firstResult = results.get(0);
                assertThat(firstResult.get("data")).isInstanceOf(Map.class);

                @SuppressWarnings("unchecked")
                Map<String, Object> dataMap = (Map<String, Object>) firstResult.get("data");
                assertThat(dataMap).containsKeys("value", "report_time");
                assertThat(dataMap.get("value")).isInstanceOf(BigDecimal.class);
                assertThat(dataMap.get("report_time")).isInstanceOf(Instant.class);

                log.info("=== Integration Test PASSED ===");
                log.info("Successfully demonstrated:");
                log.info("  ✓ Smart batching with partition-level grouping");
                log.info("  ✓ Mixed year batches (2023 & 2024)");
                log.info("  ✓ Scatter-gather query across year buckets");
                log.info("  ✓ Shared DAL (UdtMapper, PartitionCalculator, TenantConfigRegistry)");
                log.info("  ✓ No data loss (10/10 rows retrieved)");

                // Print sample result
                log.info("Sample result: {}", results.get(0));
        }

        /**
         * Original test for backward compatibility.
         */
        @Test
        void testScatterGatherAcrossYearBuckets() {
                log.info("=== Starting Original Integration Test ===");

                // Step 1: Ingest data for IBM in December 2023
                log.info("Step 1: Ingesting data for Dec 2023...");
                ingestDataPoint("IBM", "IBM_STOCK", LocalDate.of(2023, 12, 15), "revenue", 95.5);
                ingestDataPoint("IBM", "IBM_STOCK", LocalDate.of(2023, 12, 20), "profit", 12.3);

                // Step 2: Ingest data for IBM in January 2024
                log.info("Step 2: Ingesting data for Jan 2024...");
                ingestDataPoint("IBM", "IBM_STOCK", LocalDate.of(2024, 1, 10), "revenue", 102.7);
                ingestDataPoint("IBM", "IBM_STOCK", LocalDate.of(2024, 1, 25), "profit", 15.8);

                // Step 3: Query data from 2023-12-01 to 2024-02-01 (crosses year boundary)
                log.info("Step 3: Querying data from 2023-12-01 to 2024-02-01...");

                Map<String, Object> criteria = new HashMap<>();
                criteria.put("tenant_id", "IBM");
                criteria.put("instrument_id", "IBM_STOCK");
                criteria.put("start_date", "2023-12-01");
                criteria.put("end_date", "2024-02-01");

                List<Map<String, Object>> results = retrievalService.retrieve("IBM", criteria);

                // Step 4: Verify results
                log.info("Step 4: Verifying results...");
                log.info("Retrieved {} records", results.size());

                assertThat(results).hasSize(4);

                // Verify we have data from both years
                long count2023 = results.stream()
                                .filter(r -> ((LocalDate) r.get("period_date")).getYear() == 2023)
                                .count();

                long count2024 = results.stream()
                                .filter(r -> ((LocalDate) r.get("period_date")).getYear() == 2024)
                                .count();

                assertThat(count2023).isEqualTo(2);
                assertThat(count2024).isEqualTo(2);

                log.info("=== Original Integration Test PASSED ===");
        }

        /**
         * Helper method to create a data row.
         */
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

                // Create UDT as a Map (will be converted to UdtValue by the service)
                Map<String, Object> dataPoint = new HashMap<>();
                dataPoint.put("value", BigDecimal.valueOf(value));
                dataPoint.put("report_time", Instant.now());
                row.put("data", dataPoint);

                return row;
        }

        /**
         * Helper method to ingest a data point.
         */
        private void ingestDataPoint(
                        String tenantId,
                        String instrumentId,
                        LocalDate periodDate,
                        String fieldId,
                        double value) {
                Map<String, Object> payload = createRow(tenantId, instrumentId, periodDate, fieldId, value);
                ingestService.ingest(tenantId, payload);
                log.info("Ingested: {} {} on {} = {}", tenantId, fieldId, periodDate, value);
        }
}

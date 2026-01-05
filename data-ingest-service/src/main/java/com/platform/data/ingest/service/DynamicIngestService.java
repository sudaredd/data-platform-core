package com.platform.data.ingest.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.platform.data.common.config.TenantConfig;
import com.platform.data.common.mapper.UdtMapper;
import com.platform.data.common.model.PartitionKey;
import com.platform.data.common.registry.TenantConfigRegistry;
import com.platform.data.common.util.PartitionCalculator;
import com.platform.data.ingest.dto.IngestBatchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Dynamic ingestion service with smart batching support.
 * Features:
 * - Partition-level batching for high throughput
 * - Automatic UDT conversion using shared mapper
 * - Polymorphic routing via TenantConfigRegistry
 */
@Service
public class DynamicIngestService {

    private static final Logger log = LoggerFactory.getLogger(DynamicIngestService.class);

    private final CqlSession session;
    private final TenantConfigRegistry registry;

    public DynamicIngestService(CqlSession session, TenantConfigRegistry registry) {
        this.session = session;
        this.registry = registry;
    }

    /**
     * Ingests a single row (legacy method for backward compatibility).
     */
    public void ingest(String tenantId, Map<String, Object> payload) {
        // For single row, create a batch request with one item
        IngestBatchRequest request = new IngestBatchRequest(
                tenantId,
                "DAILY", // Default periodicity
                List.of(payload));

        ingestBatchAsync(request).join();
    }

    /**
     * Ingests a batch of rows with partition-level batching.
     * 
     * @param request Batch request containing tenant metadata and data
     * @return CompletableFuture that completes when all batches are written
     */
    public CompletableFuture<Void> ingestBatchAsync(IngestBatchRequest request) {
        request.validate();

        log.info("Processing batch for tenant: {}, periodicity: {}, rows: {}",
                request.tenantId(), request.periodicity(), request.data().size());

        // Step 1: Infer data type from first row
        String dataType = inferDataType(request.data().get(0));
        log.debug("Inferred data type: {}", dataType);

        // Step 2: Lookup configuration
        TenantConfig config = registry.lookup(request.tenantId(), request.periodicity(), dataType);

        // Step 3: Group rows by partition
        Map<PartitionKey, List<BoundStatement>> groups = groupByPartition(config, request.data());
        log.info("Grouped {} rows into {} partition batches", request.data().size(), groups.size());

        // Step 4: Execute batches asynchronously
        List<CompletableFuture<AsyncResultSet>> futures = new ArrayList<>();

        for (Map.Entry<PartitionKey, List<BoundStatement>> entry : groups.entrySet()) {
            // Build batch by adding statements one by one
            var batchBuilder = BatchStatement.builder(DefaultBatchType.LOGGED);
            for (BoundStatement stmt : entry.getValue()) {
                batchBuilder.addStatement(stmt);
            }
            BatchStatement batch = batchBuilder.build();

            CompletableFuture<AsyncResultSet> future = session.executeAsync(batch).toCompletableFuture();
            futures.add(future);

            log.debug("Submitted batch for partition: {} with {} statements",
                    entry.getKey(), entry.getValue().size());
        }

        // Step 5: Wait for all batches to complete
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> log.info("Batch ingestion complete for tenant: {}", request.tenantId()));
    }

    /**
     * Infers the data type from the first row.
     */
    private String inferDataType(Map<String, Object> firstRow) {
        // Look for a "data" field (common UDT field name)
        Object dataField = firstRow.get("data");

        if (dataField instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> dataMap = (Map<String, Object>) dataField;
            Object value = dataMap.get("value");

            if (value instanceof Number) {
                return "NUMERIC";
            } else if (value instanceof String) {
                return "STRING";
            }
        }

        // Default to NUMERIC
        return "NUMERIC";
    }

    /**
     * Groups rows by partition key.
     */
    private Map<PartitionKey, List<BoundStatement>> groupByPartition(
            TenantConfig config,
            List<Map<String, Object>> data) {
        Map<PartitionKey, List<BoundStatement>> groups = new HashMap<>();

        for (Map<String, Object> row : data) {
            // Create a mutable copy
            Map<String, Object> enrichedRow = new HashMap<>(row);

            // Calculate and inject bucket if configured
            Object bucket = PartitionCalculator.calculateBucket(config, enrichedRow);
            if (bucket != null) {
                enrichedRow.put(config.getBucketColumnOrThrow(), bucket);
            }

            // Process UDTs using shared mapper
            for (String udtColumn : config.udtColumns()) {
                if (enrichedRow.containsKey(udtColumn) && enrichedRow.get(udtColumn) instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> udtMap = (Map<String, Object>) enrichedRow.get(udtColumn);
                    UdtValue udtValue = UdtMapper.toUdt(session, config.keyspace(), udtColumn, udtMap);
                    enrichedRow.put(udtColumn, udtValue);
                }
            }

            // Extract partition key values
            List<Object> pkValues = config.partitionKeys().stream()
                    .map(enrichedRow::get)
                    .toList();
            PartitionKey pk = new PartitionKey(pkValues);

            // Build insert statement
            BoundStatement stmt = buildInsertStatement(config, enrichedRow);

            // Group by partition
            groups.computeIfAbsent(pk, k -> new ArrayList<>()).add(stmt);
        }

        return groups;
    }

    /**
     * Builds an insert statement for a single row.
     */
    private BoundStatement buildInsertStatement(TenantConfig config, Map<String, Object> payload) {
        // Build the insert query
        var insertInto = QueryBuilder.insertInto(config.keyspace(), config.tableName());

        // Start with the first column
        var iterator = payload.entrySet().iterator();
        if (!iterator.hasNext()) {
            throw new IllegalArgumentException("Payload cannot be empty");
        }

        var firstEntry = iterator.next();
        var regularInsert = insertInto.value(firstEntry.getKey(), QueryBuilder.bindMarker(firstEntry.getKey()));

        // Chain the rest of the values
        while (iterator.hasNext()) {
            var entry = iterator.next();
            regularInsert = regularInsert.value(entry.getKey(), QueryBuilder.bindMarker(entry.getKey()));
        }

        // Prepare the statement
        PreparedStatement prepared = session.prepare(regularInsert.build());

        // Bind the values
        BoundStatement bound = prepared.boundStatementBuilder().build();

        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof String s) {
                bound = bound.setString(key, s);
            } else if (value instanceof Integer i) {
                bound = bound.setInt(key, i);
            } else if (value instanceof LocalDate ld) {
                bound = bound.setLocalDate(key, ld);
            } else if (value instanceof UdtValue udt) {
                bound = bound.setUdtValue(key, udt);
            } else if (value instanceof BigDecimal bd) {
                bound = bound.setBigDecimal(key, bd);
            } else {
                log.warn("Unsupported type for column {}: {}", key, value.getClass());
            }
        }

        return bound;
    }
}

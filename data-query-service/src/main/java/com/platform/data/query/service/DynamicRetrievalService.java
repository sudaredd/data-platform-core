package com.platform.data.query.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.platform.data.common.config.TenantConfig;
import com.platform.data.common.mapper.UdtMapper;
import com.platform.data.common.registry.TenantConfigRegistry;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Dynamic retrieval service implementing scatter-gather pattern for bucketed
 * queries. Features: -
 * Parallel async queries across year buckets - Automatic UDT to Map conversion
 * using shared mapper
 * - Polymorphic routing via TenantConfigRegistry
 */
@Service
public class DynamicRetrievalService {

  private static final Logger log = LoggerFactory.getLogger(DynamicRetrievalService.class);

  private final CqlSession session;
  private final TenantConfigRegistry registry;

  public DynamicRetrievalService(CqlSession session, TenantConfigRegistry registry) {
    this.session = session;
    this.registry = registry;
  }

  /**
   * Retrieves data for a specific tenant based on criteria. Implements
   * scatter-gather pattern for
   * bucketed tables.
   *
   * @param tenantId The tenant identifier
   * @param criteria Query criteria (must include start_date and end_date)
   * @return List of results as Map<String, Object>
   */
  public List<Map<String, Object>> retrieve(String tenantId, Map<String, Object> criteria) {
    // Extract date range
    LocalDate startDate = extractDate(criteria, "start_date");
    LocalDate endDate = extractDate(criteria, "end_date");

    if (startDate == null || endDate == null) {
      throw new IllegalArgumentException("start_date and end_date are required");
    }

    // Infer data type (default to NUMERIC for now)
    String dataType = "NUMERIC";
    String periodicity = "DAILY";

    // Lookup configuration
    TenantConfig config = registry.lookup(tenantId, periodicity, dataType);

    // Inject tenant_id into criteria for partition key filtering
    Map<String, Object> enrichedCriteria = new HashMap<>(criteria);
    enrichedCriteria.put("tenant_id", tenantId);

    // Scatter-gather based on bucket configuration
    if (config.hasBucket()) {
      return scatterGatherQuery(config, enrichedCriteria, startDate, endDate);
    } else {
      return singleQuery(config, enrichedCriteria, startDate, endDate);
    }
  }

  /** Executes parallel queries across year buckets and gathers results. */
  private List<Map<String, Object>> scatterGatherQuery(
      TenantConfig config, Map<String, Object> criteria, LocalDate startDate, LocalDate endDate) {
    int startYear = startDate.getYear();
    int endYear = endDate.getYear();

    log.info(
        "Scatter-gather query from {} to {} ({} buckets)",
        startYear,
        endYear,
        (endYear - startYear + 1));

    // Create async queries for each year bucket
    List<CompletableFuture<List<Row>>> futures = new ArrayList<>();

    for (int year = startYear; year <= endYear; year++) {
      Map<String, Object> bucketCriteria = new HashMap<>(criteria);
      bucketCriteria.put(config.getBucketColumnOrThrow(), year);

      CompletableFuture<List<Row>> future = executeAsyncQuery(config, bucketCriteria, startDate, endDate);
      futures.add(future);
    }

    // Gather all results
    List<Row> allRows = futures.stream()
        .map(CompletableFuture::join)
        .flatMap(List::stream)
        .collect(Collectors.toList());

    log.info("Gathered {} total rows from {} buckets", allRows.size(), futures.size());

    // Convert rows to maps using shared mapper
    return allRows.stream().map(row -> convertRowToMap(row, config)).collect(Collectors.toList());
  }

  /** Executes a single query (no bucketing). */
  private List<Map<String, Object>> singleQuery(
      TenantConfig config, Map<String, Object> criteria, LocalDate startDate, LocalDate endDate) {
    log.info("Single query (no bucketing)");

    List<Row> rows = executeAsyncQuery(config, criteria, startDate, endDate).join();

    return rows.stream().map(row -> convertRowToMap(row, config)).collect(Collectors.toList());
  }

  /** Executes an async query and returns a future of rows. */
  private CompletableFuture<List<Row>> executeAsyncQuery(
      TenantConfig config, Map<String, Object> criteria, LocalDate startDate, LocalDate endDate) {
    // Build the select query
    Select select = QueryBuilder.selectFrom(config.keyspace(), config.tableName()).all();

    // Add WHERE clauses for partition keys
    for (String partitionKey : config.partitionKeys()) {
      Object value = criteria.get(partitionKey);
      if (value != null) {
        select = select.whereColumn(partitionKey).isEqualTo(QueryBuilder.bindMarker(partitionKey));
      }
    }

    // Note: bucket column is already part of partition keys, so no need to add it
    // again

    // Add date range filter
    select = select
        .whereColumn("period_date")
        .isGreaterThanOrEqualTo(QueryBuilder.bindMarker("start_date"))
        .whereColumn("period_date")
        .isLessThanOrEqualTo(QueryBuilder.bindMarker("end_date"));

    // Prepare and bind
    PreparedStatement prepared = session.prepare(select.build());
    BoundStatementBuilder builder = prepared.boundStatementBuilder();

    // Bind partition keys
    for (String partitionKey : config.partitionKeys()) {
      Object value = criteria.get(partitionKey);
      if (value != null) {
        if (value instanceof String s) {
          builder.setString(partitionKey, s);
        } else if (value instanceof Integer i) {
          builder.setInt(partitionKey, i);
        }
      }
    }

    // Note: bucket column binding is already handled by partition keys loop above

    // Bind date range
    builder.setLocalDate("start_date", startDate);
    builder.setLocalDate("end_date", endDate);

    BoundStatement bound = builder.build();

    // Execute async
    return session
        .executeAsync(bound)
        .toCompletableFuture()
        .thenApply(
            rs -> {
              List<Row> rows = new ArrayList<>();
              for (Row row : rs.currentPage()) {
                rows.add(row);
              }
              return rows;
            });
  }

  /**
   * Converts a Cassandra Row to a Map, recursively converting UDTs using shared
   * mapper.
   */
  private Map<String, Object> convertRowToMap(Row row, TenantConfig config) {
    Map<String, Object> result = new HashMap<>();

    for (int i = 0; i < row.getColumnDefinitions().size(); i++) {
      String columnName = row.getColumnDefinitions().get(i).getName().asInternal();
      Object value = row.getObject(i);

      if (value == null) {
        result.put(columnName, null);
      } else if (value instanceof UdtValue udtValue) {
        // Use shared UdtMapper for conversion
        result.put(columnName, UdtMapper.toMap(udtValue));
      } else {
        result.put(columnName, value);
      }
    }

    return result;
  }

  /** Extracts a LocalDate from criteria. */
  private LocalDate extractDate(Map<String, Object> criteria, String key) {
    Object value = criteria.get(key);
    if (value instanceof LocalDate ld) {
      return ld;
    } else if (value instanceof String str) {
      return LocalDate.parse(str);
    }
    return null;
  }
}

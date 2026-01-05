package com.platform.data.common.config;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Runtime metadata configuration for a tenant's table.
 * This record stores all necessary information to construct dynamic CQL queries.
 */
public record TenantConfig(
    String keyspace,
    String tableName,
    List<String> partitionKeys,
    Optional<String> bucketColumn,
    Set<String> udtColumns
) {
    /**
     * Creates a TenantConfig without a bucket column.
     */
    public static TenantConfig withoutBucket(
        String keyspace,
        String tableName,
        List<String> partitionKeys,
        Set<String> udtColumns
    ) {
        return new TenantConfig(keyspace, tableName, partitionKeys, Optional.empty(), udtColumns);
    }

    /**
     * Creates a TenantConfig with a bucket column.
     */
    public static TenantConfig withBucket(
        String keyspace,
        String tableName,
        List<String> partitionKeys,
        String bucketColumn,
        Set<String> udtColumns
    ) {
        return new TenantConfig(keyspace, tableName, partitionKeys, Optional.of(bucketColumn), udtColumns);
    }

    /**
     * Checks if this configuration uses bucketing.
     */
    public boolean hasBucket() {
        return bucketColumn.isPresent();
    }

    /**
     * Gets the bucket column name, or throws if not present.
     */
    public String getBucketColumnOrThrow() {
        return bucketColumn.orElseThrow(() -> 
            new IllegalStateException("Bucket column not configured for this tenant"));
    }

    /**
     * Checks if a column is a UDT.
     */
    public boolean isUdtColumn(String columnName) {
        return udtColumns.contains(columnName);
    }
}

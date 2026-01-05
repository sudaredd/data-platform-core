package com.platform.data.common.util;

import com.platform.data.common.config.TenantConfig;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Map;

/**
 * Centralized partition bucket calculation utility.
 * Ensures consistent partitioning logic across ingest and query services.
 */
public class PartitionCalculator {

    /**
     * Calculates the bucket value for a given payload.
     * 
     * @param config  Tenant configuration
     * @param payload Data payload
     * @return Bucket value (e.g., year as Integer), or null if no bucket configured
     */
    public static Object calculateBucket(TenantConfig config, Map<String, Object> payload) {
        if (!config.hasBucket()) {
            return null;
        }

        String bucketColumn = config.getBucketColumnOrThrow();

        // For year-based bucketing, look for a date field
        // Common date field names: period_date, date, timestamp, etc.
        Object dateValue = findDateValue(payload);

        if (dateValue == null) {
            // Return null instead of throwing exception for missing date
            return null;
        }

        return extractYear(dateValue);
    }

    /**
     * Finds a date value in the payload.
     * Looks for common date field names.
     */
    private static Object findDateValue(Map<String, Object> payload) {
        // Try common field names
        String[] dateFields = { "period_date", "date", "timestamp", "report_date", "event_date" };

        for (String field : dateFields) {
            if (payload.containsKey(field)) {
                return payload.get(field);
            }
        }

        return null;
    }

    /**
     * Extracts the year from a date value.
     */
    private static int extractYear(Object dateValue) {
        if (dateValue instanceof LocalDate localDate) {
            return localDate.getYear();
        } else if (dateValue instanceof String dateStr) {
            LocalDate date = LocalDate.parse(dateStr);
            return date.getYear();
        } else if (dateValue instanceof Long timestamp) {
            return Instant.ofEpochMilli(timestamp)
                    .atZone(ZoneId.systemDefault())
                    .getYear();
        } else if (dateValue instanceof Instant instant) {
            return instant.atZone(ZoneId.systemDefault()).getYear();
        }

        throw new IllegalArgumentException(
                "Unsupported date type for bucket calculation: " + dateValue.getClass());
    }

    /**
     * Calculates the year range for a date range query.
     * Used by query service for scatter-gather.
     * 
     * @param startDate Start date
     * @param endDate   End date
     * @return Array of all years in the range [startYear, startYear+1, ...,
     *         endYear]
     */
    public static int[] calculateYearRange(LocalDate startDate, LocalDate endDate) {
        int startYear = startDate.getYear();
        int endYear = endDate.getYear();

        int[] years = new int[endYear - startYear + 1];
        for (int i = 0; i < years.length; i++) {
            years[i] = startYear + i;
        }

        return years;
    }
}

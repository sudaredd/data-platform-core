package com.platform.data.common.util;

import com.platform.data.common.config.TenantConfig;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for PartitionCalculator.
 */
class PartitionCalculatorTest {

    @Test
    void testCalculateBucket_extractsYearFromDate() {
        // Arrange
        TenantConfig config = TenantConfig.withBucket(
                "test_ks",
                "test_table",
                List.of("tenant_id", "period_year"),
                "period_year",
                Set.of());

        Map<String, Object> payload = new HashMap<>();
        payload.put("period_date", LocalDate.of(2024, 6, 15));

        // Act
        Object bucket = PartitionCalculator.calculateBucket(config, payload);

        // Assert
        assertThat(bucket).isEqualTo(2024);
    }

    @Test
    void testCalculateBucket_withNoBucketColumn_returnsNull() {
        // Arrange
        TenantConfig config = new TenantConfig(
                "test_ks",
                "test_table",
                List.of("tenant_id"),
                null,
                Set.of());

        Map<String, Object> payload = new HashMap<>();
        payload.put("period_date", LocalDate.of(2024, 6, 15));

        // Act
        Object bucket = PartitionCalculator.calculateBucket(config, payload);

        // Assert
        assertThat(bucket).isNull();
    }

    @Test
    void testCalculateBucket_withMissingDateField_returnsNull() {
        // Arrange
        TenantConfig config = TenantConfig.withBucket(
                "test_ks",
                "test_table",
                List.of("tenant_id", "period_year"),
                "period_year",
                Set.of());

        Map<String, Object> payload = new HashMap<>();
        // No period_date field

        // Act
        Object bucket = PartitionCalculator.calculateBucket(config, payload);

        // Assert
        assertThat(bucket).isNull();
    }

    @Test
    void testCalculateYearRange_singleYear() {
        // Arrange
        LocalDate startDate = LocalDate.of(2024, 1, 1);
        LocalDate endDate = LocalDate.of(2024, 12, 31);

        // Act
        int[] years = PartitionCalculator.calculateYearRange(startDate, endDate);

        // Assert
        assertThat(years).containsExactly(2024);
    }

    @Test
    void testCalculateYearRange_multipleYears() {
        // Arrange
        LocalDate startDate = LocalDate.of(2023, 6, 1);
        LocalDate endDate = LocalDate.of(2025, 3, 31);

        // Act
        int[] years = PartitionCalculator.calculateYearRange(startDate, endDate);

        // Assert
        assertThat(years).containsExactly(2023, 2024, 2025);
    }

    @Test
    void testCalculateYearRange_acrossYearBoundary() {
        // Arrange
        LocalDate startDate = LocalDate.of(2023, 12, 15);
        LocalDate endDate = LocalDate.of(2024, 1, 15);

        // Act
        int[] years = PartitionCalculator.calculateYearRange(startDate, endDate);

        // Assert
        assertThat(years).containsExactly(2023, 2024);
    }
}

package com.platform.data.common.mapper;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for UdtMapper.
 * 
 * Note: These tests focus on the toMap() functionality which doesn't require
 * complex mocking. The toUdt() method requires extensive Cassandra type mocking
 * and is better tested via integration tests.
 */
class UdtMapperTest {

    @Test
    void testToMap_withNullUdtValue_returnsEmptyMap() {
        // Act
        Map<String, Object> result = UdtMapper.toMap(null);

        // Assert
        assertThat(result).isEmpty();
    }

    @Test
    void testToMap_withValidUdtValue_convertsToMap() {
        // Arrange
        UserDefinedType udtType = mock(UserDefinedType.class);
        UdtValue udtValue = mock(UdtValue.class);

        when(udtValue.getType()).thenReturn(udtType);
        when(udtType.getFieldNames()).thenReturn(List.of(
                com.datastax.oss.driver.api.core.CqlIdentifier.fromCql("value"),
                com.datastax.oss.driver.api.core.CqlIdentifier.fromCql("report_time")));
        when(udtValue.getBigDecimal("value")).thenReturn(BigDecimal.valueOf(100.5));
        when(udtValue.getInstant("report_time")).thenReturn(Instant.parse("2024-01-01T12:00:00Z"));

        // Act
        Map<String, Object> result = UdtMapper.toMap(udtValue);

        // Assert
        assertThat(result).isNotNull();
        assertThat(result).containsKeys("value", "report_time");
        assertThat(result.get("value")).isEqualTo(BigDecimal.valueOf(100.5));
        assertThat(result.get("report_time")).isEqualTo(Instant.parse("2024-01-01T12:00:00Z"));
    }

    @Test
    void testToMap_withNestedUdt_handlesRecursively() {
        // Arrange
        UserDefinedType outerType = mock(UserDefinedType.class);
        UserDefinedType innerType = mock(UserDefinedType.class);
        UdtValue outerValue = mock(UdtValue.class);
        UdtValue innerValue = mock(UdtValue.class);

        when(outerValue.getType()).thenReturn(outerType);
        when(innerValue.getType()).thenReturn(innerType);

        when(outerType.getFieldNames()).thenReturn(List.of(
                com.datastax.oss.driver.api.core.CqlIdentifier.fromCql("nested")));
        when(innerType.getFieldNames()).thenReturn(List.of(
                com.datastax.oss.driver.api.core.CqlIdentifier.fromCql("value")));

        when(outerValue.getUdtValue("nested")).thenReturn(innerValue);
        when(innerValue.getBigDecimal("value")).thenReturn(BigDecimal.valueOf(42.0));

        // Act
        Map<String, Object> result = UdtMapper.toMap(outerValue);

        // Assert
        assertThat(result).containsKey("nested");
        assertThat(result.get("nested")).isInstanceOf(Map.class);

        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) result.get("nested");
        assertThat(nested.get("value")).isEqualTo(BigDecimal.valueOf(42.0));
    }

    @Test
    void testToMap_withNullField_includesNullInMap() {
        // Arrange
        UserDefinedType udtType = mock(UserDefinedType.class);
        UdtValue udtValue = mock(UdtValue.class);

        when(udtValue.getType()).thenReturn(udtType);
        when(udtType.getFieldNames()).thenReturn(List.of(
                com.datastax.oss.driver.api.core.CqlIdentifier.fromCql("value")));
        when(udtValue.getBigDecimal("value")).thenReturn(null);

        // Act
        Map<String, Object> result = UdtMapper.toMap(udtValue);

        // Assert
        assertThat(result).containsKey("value");
        assertThat(result.get("value")).isNull();
    }
}

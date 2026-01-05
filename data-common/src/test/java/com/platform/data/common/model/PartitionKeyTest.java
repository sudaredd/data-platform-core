package com.platform.data.common.model;

import static org.assertj.core.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit tests for PartitionKey. */
class PartitionKeyTest {

  @Test
  void testEquals_sameValues_returnsTrue() {
    // Arrange
    PartitionKey key1 = new PartitionKey(List.of("IBM", "IBM_STOCK", 2024));
    PartitionKey key2 = new PartitionKey(List.of("IBM", "IBM_STOCK", 2024));

    // Act & Assert
    assertThat(key1).isEqualTo(key2);
  }

  @Test
  void testEquals_differentValues_returnsFalse() {
    // Arrange
    PartitionKey key1 = new PartitionKey(List.of("IBM", "IBM_STOCK", 2024));
    PartitionKey key2 = new PartitionKey(List.of("IBM", "IBM_STOCK", 2023));

    // Act & Assert
    assertThat(key1).isNotEqualTo(key2);
  }

  @Test
  void testHashCode_sameValues_returnsSameHash() {
    // Arrange
    PartitionKey key1 = new PartitionKey(List.of("IBM", "IBM_STOCK", 2024));
    PartitionKey key2 = new PartitionKey(List.of("IBM", "IBM_STOCK", 2024));

    // Act & Assert
    assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
  }

  @Test
  void testAsMapKey_groupsCorrectly() {
    // Arrange
    Map<PartitionKey, String> map = new HashMap<>();
    PartitionKey key1 = new PartitionKey(List.of("IBM", 2024));
    PartitionKey key2 = new PartitionKey(List.of("IBM", 2024));
    PartitionKey key3 = new PartitionKey(List.of("IBM", 2023));

    // Act
    map.put(key1, "value1");
    map.put(key2, "value2"); // Should overwrite value1
    map.put(key3, "value3");

    // Assert
    assertThat(map).hasSize(2);
    assertThat(map.get(key1)).isEqualTo("value2");
    assertThat(map.get(key3)).isEqualTo("value3");
  }

  @Test
  void testToString_containsValues() {
    // Arrange
    PartitionKey key = new PartitionKey(List.of("IBM", "IBM_STOCK", 2024));

    // Act
    String result = key.toString();

    // Assert
    assertThat(result).contains("IBM", "IBM_STOCK", "2024");
  }
}

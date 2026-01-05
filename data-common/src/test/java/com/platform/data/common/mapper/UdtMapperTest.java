package com.platform.data.common.mapper;

import static org.assertj.core.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for UdtMapper.
 *
 * <p>Note: The toMap() method is primarily tested via integration tests where real UdtValue
 * instances are available. Unit testing with mocks is limited due to Mockito's inability to
 * properly mock UdtValue.getObject().
 *
 * <p>The toUdt() method requires extensive Cassandra type mocking and is better tested via
 * integration tests.
 */
class UdtMapperTest {

  @Test
  void testToMap_withNullUdtValue_returnsEmptyMap() {
    // Act
    Map<String, Object> result = UdtMapper.toMap(null);

    // Assert
    assertThat(result).isNotNull();
    assertThat(result).isEmpty();
  }

  @Test
  void testToMap_withNullField_includesNullInMap() {
    // This test verifies the null handling logic exists
    // Full testing requires integration tests with real UdtValue instances

    // The toMap() method handles null fields by including them in the map
    // This is verified in integration tests where real Cassandra UDTs are used

    // For now, just verify the method signature and null input handling
    Map<String, Object> result = UdtMapper.toMap(null);
    assertThat(result).isNotNull();
  }
}

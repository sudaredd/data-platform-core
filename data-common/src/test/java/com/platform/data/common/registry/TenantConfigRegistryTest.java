package com.platform.data.common.registry;

import static org.assertj.core.api.Assertions.*;

import com.platform.data.common.config.TenantConfig;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for TenantConfigRegistry. */
class TenantConfigRegistryTest {

  private TenantConfigRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new TenantConfigRegistry();
  }

  @Test
  void testRegisterAndLookup_success() {
    // Arrange
    TenantConfig config =
        TenantConfig.withBucket(
            "test_ks", "test_table", List.of("tenant_id"), "period_year", Set.of("data"));

    // Act
    registry.register("IBM", "DAILY", "NUMERIC", config);
    TenantConfig result = registry.lookup("IBM", "DAILY", "NUMERIC");

    // Assert
    assertThat(result).isEqualTo(config);
  }

  @Test
  void testPolymorphicRouting_differentDataTypes() {
    // Arrange
    TenantConfig numericConfig =
        TenantConfig.withBucket("test_ks", "numeric_table", List.of("pk"), "period_year", Set.of());
    TenantConfig stringConfig =
        TenantConfig.withBucket("test_ks", "string_table", List.of("pk"), "period_year", Set.of());

    // Act
    registry.register("IBM", "DAILY", "NUMERIC", numericConfig);
    registry.register("IBM", "DAILY", "STRING", stringConfig);

    // Assert
    assertThat(registry.lookup("IBM", "DAILY", "NUMERIC")).isEqualTo(numericConfig);
    assertThat(registry.lookup("IBM", "DAILY", "STRING")).isEqualTo(stringConfig);
  }

  @Test
  void testLookup_nonExistent_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> registry.lookup("UNKNOWN", "DAILY", "NUMERIC"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No configuration found");
  }

  @Test
  void testExists_returnsTrue_whenConfigExists() {
    // Arrange
    TenantConfig config =
        TenantConfig.withBucket("test_ks", "test_table", List.of("pk"), "period_year", Set.of());
    registry.register("IBM", "DAILY", "NUMERIC", config);

    // Act & Assert
    assertThat(registry.exists("IBM", "DAILY", "NUMERIC")).isTrue();
  }

  @Test
  void testExists_returnsFalse_whenConfigDoesNotExist() {
    // Act & Assert
    assertThat(registry.exists("UNKNOWN", "DAILY", "NUMERIC")).isFalse();
  }

  @Test
  void testUnregister_removesConfig() {
    // Arrange
    TenantConfig config =
        TenantConfig.withBucket("test_ks", "test_table", List.of("pk"), "period_year", Set.of());
    registry.register("IBM", "DAILY", "NUMERIC", config);

    // Act
    registry.unregister("IBM", "DAILY", "NUMERIC");

    // Assert
    assertThat(registry.exists("IBM", "DAILY", "NUMERIC")).isFalse();
  }

  @Test
  void testClear_removesAllConfigs() {
    // Arrange
    TenantConfig config1 =
        TenantConfig.withBucket("test_ks", "table1", List.of("pk"), "period_year", Set.of());
    TenantConfig config2 =
        TenantConfig.withBucket("test_ks", "table2", List.of("pk"), "period_year", Set.of());
    registry.register("IBM", "DAILY", "NUMERIC", config1);
    registry.register("MSFT", "MONTHLY", "STRING", config2);

    // Act
    registry.clear();

    // Assert
    assertThat(registry.exists("IBM", "DAILY", "NUMERIC")).isFalse();
    assertThat(registry.exists("MSFT", "MONTHLY", "STRING")).isFalse();
  }
}

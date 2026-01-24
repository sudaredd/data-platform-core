package com.platform.data.common.registry;

import com.platform.data.common.config.TenantConfig;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;

/**
 * Centralized tenant configuration registry with polymorphic routing support.
 * Supports multiple
 * table types per tenant based on (tenantId, periodicity, dataType).
 */
@Component
public class TenantConfigRegistry {

  private static final Logger log = LoggerFactory.getLogger(TenantConfigRegistry.class);

  private final Map<RegistryKey, TenantConfig> configs = new ConcurrentHashMap<>();

  /**
   * Registers a tenant configuration.
   *
   * @param tenantId    Tenant identifier
   * @param periodicity Data periodicity (e.g., "DAILY", "MONTHLY")
   * @param dataType    Data type (e.g., "NUMERIC", "STRING")
   * @param config      Tenant configuration
   */
  public void register(String tenantId, String periodicity, String dataType, TenantConfig config) {
    RegistryKey key = new RegistryKey(tenantId, periodicity, dataType);
    configs.put(key, config);
    log.info("Registered config: {} -> {}.{}", key, config.keyspace(), config.tableName());
  }

  /**
   * Looks up a tenant configuration.
   *
   * @param tenantId    Tenant identifier
   * @param periodicity Data periodicity
   * @param dataType    Data type
   * @return Tenant configuration
   * @throws IllegalArgumentException if config not found
   */
  public TenantConfig lookup(String tenantId, String periodicity, String dataType) {
    RegistryKey key = new RegistryKey(tenantId, periodicity, dataType);
    TenantConfig config = configs.get(key);

    if (config == null) {
      throw new IllegalArgumentException(
          "No configuration found for: " + key + ". Available configs: " + configs.keySet());
    }

    return config;
  }

  /** Checks if a configuration exists. */
  public boolean exists(String tenantId, String periodicity, String dataType) {
    RegistryKey key = new RegistryKey(tenantId, periodicity, dataType);
    return configs.containsKey(key);
  }

  /** Removes a configuration. */
  public void unregister(String tenantId, String periodicity, String dataType) {
    RegistryKey key = new RegistryKey(tenantId, periodicity, dataType);
    configs.remove(key);
    log.info("Unregistered config: {}", key);
  }

  /** Clears all configurations (useful for testing). */
  public void clear() {
    configs.clear();
    log.info("Cleared all configurations");
  }

  /** Composite key for registry lookup. */
  private record RegistryKey(String tenantId, String periodicity, String dataType) {
    @Override
    public String toString() {
      return String.format("(%s, %s, %s)", tenantId, periodicity, dataType);
    }
  }
}

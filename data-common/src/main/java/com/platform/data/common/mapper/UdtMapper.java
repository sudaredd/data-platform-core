package com.platform.data.common.mapper;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Centralized UDT conversion utility. Ensures consistent UDT ↔ Map conversion across ingest and
 * query services.
 */
public class UdtMapper {

  private static final Logger log = LoggerFactory.getLogger(UdtMapper.class);

  /**
   * Converts a Map to a UdtValue. Handles BigDecimal, Instant, String parsing, and nested UDTs.
   *
   * @param session CQL session for metadata access
   * @param keyspace Keyspace name
   * @param udtName UDT type name
   * @param data Map containing field values
   * @return UdtValue instance
   */
  public static UdtValue toUdt(
      CqlSession session, String keyspace, String udtName, Map<String, Object> data) {
    // Get the UDT metadata from the session
    UserDefinedType udtType =
        session
            .getMetadata()
            .getKeyspace(keyspace)
            .orElseThrow(() -> new IllegalStateException("Keyspace not found: " + keyspace))
            .getUserDefinedType(udtName)
            .orElseThrow(() -> new IllegalStateException("UDT not found: " + udtName));

    UdtValue udtValue = udtType.newValue();

    // Populate the UDT fields
    for (Map.Entry<String, Object> field : data.entrySet()) {
      String fieldName = field.getKey();
      Object fieldValue = field.getValue();

      if (fieldValue == null) {
        // Skip null values
        continue;
      }

      udtValue = setUdtField(session, keyspace, udtValue, fieldName, fieldValue);
    }

    return udtValue;
  }

  /** Sets a single field in a UdtValue, handling type conversion. */
  private static UdtValue setUdtField(
      CqlSession session, String keyspace, UdtValue udtValue, String fieldName, Object fieldValue) {
    if (fieldValue instanceof BigDecimal bd) {
      return udtValue.setBigDecimal(fieldName, bd);
    } else if (fieldValue instanceof Double d) {
      return udtValue.setBigDecimal(fieldName, BigDecimal.valueOf(d));
    } else if (fieldValue instanceof Integer i) {
      return udtValue.setBigDecimal(fieldName, BigDecimal.valueOf(i));
    } else if (fieldValue instanceof Long l) {
      return udtValue.setBigDecimal(fieldName, BigDecimal.valueOf(l));
    } else if (fieldValue instanceof Instant instant) {
      return udtValue.setInstant(fieldName, instant);
    } else if (fieldValue instanceof String str) {
      // Try to parse as timestamp if field name contains "time"
      if (fieldName.toLowerCase().contains("time")) {
        try {
          return udtValue.setInstant(fieldName, Instant.parse(str));
        } catch (Exception e) {
          log.warn("Failed to parse timestamp for field {}: {}", fieldName, str);
          return udtValue.setString(fieldName, str);
        }
      } else {
        return udtValue.setString(fieldName, str);
      }
    } else if (fieldValue instanceof Map) {
      // Nested UDT - recursive conversion
      @SuppressWarnings("unchecked")
      Map<String, Object> nestedMap = (Map<String, Object>) fieldValue;
      UdtValue nestedUdt = toUdt(session, keyspace, fieldName, nestedMap);
      return udtValue.setUdtValue(fieldName, nestedUdt);
    } else {
      log.warn("Unsupported UDT field type: {} for field: {}", fieldValue.getClass(), fieldName);
      return udtValue;
    }
  }

  /**
   * Converts a UdtValue to a Map. Recursively handles nested UDTs for clean JSON serialization.
   *
   * @param udtValue UDT to convert
   * @return Map representation
   */
  public static Map<String, Object> toMap(UdtValue udtValue) {
    if (udtValue == null) {
      return new HashMap<>();
    }

    Map<String, Object> map = new HashMap<>();

    for (int i = 0; i < udtValue.getType().getFieldNames().size(); i++) {
      String fieldName = udtValue.getType().getFieldNames().get(i).asInternal();
      Object fieldValue = udtValue.getObject(i);

      if (fieldValue == null) {
        map.put(fieldName, null);
      } else if (fieldValue instanceof UdtValue nestedUdt) {
        // Recursive conversion for nested UDTs
        map.put(fieldName, toMap(nestedUdt));
      } else {
        map.put(fieldName, fieldValue);
      }
    }

    return map;
  }
}

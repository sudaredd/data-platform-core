package com.platform.data.ingest.dto;

import java.util.List;
import java.util.Map;

/** Envelope DTO for batch ingestion requests. Contains tenant metadata and data payload. */
public record IngestBatchRequest(
    String tenantId, String periodicity, List<Map<String, Object>> data) {
  /** Validates the request. */
  public void validate() {
    if (tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("tenantId cannot be null or empty");
    }
    if (periodicity == null || periodicity.isBlank()) {
      throw new IllegalArgumentException("periodicity cannot be null or empty");
    }
    if (data == null || data.isEmpty()) {
      throw new IllegalArgumentException("data cannot be null or empty");
    }
  }
}

package com.platform.data.query.controller;

import com.platform.data.query.service.DynamicRetrievalService;
import java.util.List;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/** REST controller for data querying. */
@RestController
@RequestMapping("/api/query")
public class QueryController {

  private final DynamicRetrievalService retrievalService;

  public QueryController(DynamicRetrievalService retrievalService) {
    this.retrievalService = retrievalService;
  }

  /**
   * Queries data for a specific tenant and periodicity.
   *
   * @param tenantId    The tenant identifier
   * @param periodicity The data periodicity (e.g., DAILY, MONTHLY)
   * @param criteria    Query criteria (must include start_date and end_date)
   * @return List of matching records
   */
  @PostMapping("/{tenantId}/{periodicity}")
  public ResponseEntity<List<Map<String, Object>>> query(
      @PathVariable("tenantId") String tenantId,
      @PathVariable("periodicity") String periodicity,
      @RequestBody Map<String, Object> criteria) {
    List<Map<String, Object>> results = retrievalService.retrieve(tenantId, criteria);
    return ResponseEntity.ok(results);
  }
}

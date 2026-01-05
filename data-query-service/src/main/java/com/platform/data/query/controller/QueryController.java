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
   * Queries data for a specific tenant.
   *
   * @param tenantId The tenant identifier
   * @param criteria Query criteria (must include start_date and end_date)
   * @return List of matching records
   */
  @PostMapping("/{tenantId}")
  public ResponseEntity<List<Map<String, Object>>> query(
      @PathVariable String tenantId, @RequestBody Map<String, Object> criteria) {
    List<Map<String, Object>> results = retrievalService.retrieve(tenantId, criteria);
    return ResponseEntity.ok(results);
  }
}

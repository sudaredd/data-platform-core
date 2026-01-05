package com.platform.data.ingest.controller;

import com.platform.data.ingest.dto.IngestBatchRequest;
import com.platform.data.ingest.service.DynamicIngestService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * REST controller for data ingestion.
 */
@RestController
@RequestMapping("/api/ingest")
public class IngestController {

    private final DynamicIngestService ingestService;

    public IngestController(DynamicIngestService ingestService) {
        this.ingestService = ingestService;
    }

    /**
     * Ingests a single row for a specific tenant.
     * 
     * @param tenantId The tenant identifier
     * @param payload  The data to ingest
     * @return Success response
     */
    @PostMapping("/{tenantId}")
    public ResponseEntity<Map<String, String>> ingest(
            @PathVariable String tenantId,
            @RequestBody Map<String, Object> payload) {
        ingestService.ingest(tenantId, payload);
        return ResponseEntity.ok(Map.of("status", "success", "tenant", tenantId));
    }

    /**
     * Ingests a batch of rows.
     * 
     * @param request Batch ingestion request
     * @return Success response
     */
    @PostMapping("/batch")
    public CompletableFuture<ResponseEntity<Map<String, Object>>> ingestBatch(
            @RequestBody IngestBatchRequest request) {
        return ingestService.ingestBatchAsync(request)
                .thenApply(v -> ResponseEntity.ok(Map.of(
                        "status", "success",
                        "tenant", request.tenantId(),
                        "rows", request.data().size())));
    }
}

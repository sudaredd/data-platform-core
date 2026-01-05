package com.platform.data.ingest.kafka;

import com.platform.data.ingest.dto.IngestBatchRequest;
import com.platform.data.ingest.service.DynamicIngestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

/**
 * Kafka consumer for platform-ingest topic. Processes batch ingestion requests with manual
 * acknowledgment.
 */
@Service
public class KafkaIngestConsumer {

  private static final Logger log = LoggerFactory.getLogger(KafkaIngestConsumer.class);

  private final DynamicIngestService ingestService;

  public KafkaIngestConsumer(DynamicIngestService ingestService) {
    this.ingestService = ingestService;
  }

  /**
   * Consumes batch ingestion requests from Kafka. Only acknowledges after async processing
   * completes.
   *
   * @param request Batch ingestion request
   * @param ack Manual acknowledgment
   */
  @KafkaListener(
      topics = "platform-ingest",
      groupId = "data-ingest-group",
      containerFactory = "kafkaListenerContainerFactory")
  public void consume(IngestBatchRequest request, Acknowledgment ack) {
    log.info("Received batch for tenant: {}, rows: {}", request.tenantId(), request.data().size());

    ingestService
        .ingestBatchAsync(request)
        .thenRun(
            () -> {
              ack.acknowledge();
              log.info(
                  "Batch processed and acknowledged: tenant={}, rows={}",
                  request.tenantId(),
                  request.data().size());
            })
        .exceptionally(
            ex -> {
              log.error(
                  "Batch ingestion failed for tenant: {}. Message will be redelivered.",
                  request.tenantId(),
                  ex);
              // Don't acknowledge - message will be redelivered
              return null;
            });
  }
}

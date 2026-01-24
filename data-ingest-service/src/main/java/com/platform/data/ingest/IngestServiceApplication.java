package com.platform.data.ingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Main application class for the Data Ingest Service. */
@SpringBootApplication(scanBasePackages = "com.platform.data")
public class IngestServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(IngestServiceApplication.class, args);
  }
}

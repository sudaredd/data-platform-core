package com.platform.data.query;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Main application class for the Data Query Service. */
@SpringBootApplication(scanBasePackages = "com.platform.data")
public class QueryServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(QueryServiceApplication.class, args);
  }
}

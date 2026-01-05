# Data Platform Core

A generic, multi-tenant Data Platform built with Java 21, Spring Boot 3.x, and Datastax Cassandra Driver 4.x.

## 🎯 Key Features

- **Dynamic Schema**: No static `@Table` classes - schema is configured at runtime
- **Multi-Tenant**: Each tenant can have different table structures and configurations
- **Auto-Partitioning**: Automatic bucket column injection (e.g., extracting year from dates)
- **Scatter-Gather Queries**: Parallel async queries across partition buckets
- **UDT Support**: Automatic conversion between Map ↔ UdtValue
- **Clean JSON**: All responses are JSON-serializable (no Cassandra types exposed)

## 📦 Project Structure

```
data-platform-core/
├── data-common/              # Shared utilities and configuration models
│   ├── TenantConfig.java     # Runtime metadata (keyspace, table, partition keys, UDTs)
│   └── SchemaInit.java       # Test helper with CQL schema
├── data-ingest-service/      # Write path (REST API)
│   └── DynamicIngestService  # Handles ingestion with auto-partitioning & UDT conversion
└── data-query-service/       # Read path (REST API)
    └── DynamicRetrievalService # Scatter-gather queries with parallel execution
```

## 🛠️ Tech Stack

- **Java 21** with Records and Text Blocks
- **Spring Boot 3.2.1** (Web only, no Spring Data)
- **Datastax Java Driver 4.17.0** (Raw CqlSession + QueryBuilder)
- **JUnit 5** + **Testcontainers** for integration testing
- **Maven** multi-module project

## 🚀 Quick Start

### Build the Project

```bash
cd data-platform-core
mvn clean install
```

### Run the Integration Test

The integration test proves the entire system works end-to-end:

```bash
cd data-query-service
mvn test -Dtest=GenericPlatformIntegrationTest
```

This test:
1. Starts Cassandra via Testcontainers
2. Creates the schema (UDT + bucketed table)
3. Ingests data for IBM across Dec 2023 and Jan 2024 (two different partitions)
4. Queries data from 2023-12-01 to 2024-02-01
5. Verifies scatter-gather worked and returned data from both years

### Run the Services

**Ingest Service** (port 8081):
```bash
cd data-ingest-service
mvn spring-boot:run
```

**Query Service** (port 8082):
```bash
cd data-query-service
mvn spring-boot:run
```

## 📊 Example Schema

The test schema demonstrates a typical time-series use case:

```sql
CREATE TYPE IF NOT EXISTS numeric_data_point (
    value decimal,
    report_time timestamp
);

CREATE TABLE IF NOT EXISTS DailyNumeric (
    tenant_id text,
    instrument_id text,
    period_year int,           -- Bucket column
    period_date date,
    field_id text,
    data frozen<numeric_data_point>,  -- UDT column
    PRIMARY KEY ((tenant_id, instrument_id, period_year), period_date, field_id)
);
```

## 🔧 How It Works

### 1. Tenant Configuration

```java
TenantConfig config = TenantConfig.withBucket(
    "test_keyspace",
    "DailyNumeric",
    List.of("tenant_id", "instrument_id", "period_year"),
    "period_year",  // Bucket column
    Set.of("data")  // UDT columns
);
```

### 2. Ingestion

```java
Map<String, Object> payload = Map.of(
    "tenant_id", "IBM",
    "instrument_id", "IBM_STOCK",
    "period_date", LocalDate.of(2024, 1, 10),
    "field_id", "revenue",
    "data", Map.of(
        "value", BigDecimal.valueOf(102.7),
        "report_time", Instant.now()
    )
);

ingestService.ingest("IBM", payload);
```

The service will:
- Extract the year (2024) from `period_date`
- Inject `period_year: 2024` into the payload
- Convert the `data` Map to a `UdtValue`
- Build and execute the CQL insert

### 3. Querying

```java
Map<String, Object> criteria = Map.of(
    "tenant_id", "IBM",
    "instrument_id", "IBM_STOCK",
    "start_date", "2023-12-01",
    "end_date", "2024-02-01"
);

List<Map<String, Object>> results = retrievalService.retrieve("IBM", criteria);
```

The service will:
- Calculate year range: 2023-2024
- Fire 2 parallel async queries (one per year bucket)
- Gather and merge results
- Convert all `UdtValue` back to `Map` for clean JSON

## 🧪 Testing

The `GenericPlatformIntegrationTest` is the proof that everything works:

```bash
mvn test -Dtest=GenericPlatformIntegrationTest
```

Expected output:
```
=== Integration Test PASSED ===
Successfully demonstrated:
  ✓ Multi-partition ingestion (2023 & 2024)
  ✓ Scatter-gather query across year buckets
  ✓ UDT conversion (Map -> UdtValue -> Map)
  ✓ Clean JSON-serializable results
```

## 📝 API Endpoints

### Ingest Service (8081)

**POST** `/api/ingest/{tenantId}`
```json
{
  "tenant_id": "IBM",
  "instrument_id": "IBM_STOCK",
  "period_date": "2024-01-10",
  "field_id": "revenue",
  "data": {
    "value": 102.7,
    "report_time": "2024-01-10T12:00:00Z"
  }
}
```

### Query Service (8082)

**POST** `/api/query/{tenantId}`
```json
{
  "tenant_id": "IBM",
  "instrument_id": "IBM_STOCK",
  "start_date": "2023-12-01",
  "end_date": "2024-02-01"
}
```

## 🏗️ Architecture Highlights

### No Static @Table Classes
Traditional Spring Data Cassandra requires:
```java
@Table
class MyEntity { ... }
```

This platform uses **runtime configuration** instead, allowing:
- Different tenants to use different schemas
- Schema evolution without code changes
- True multi-tenancy

### Scatter-Gather Pattern
For bucketed tables, queries automatically:
1. Calculate bucket range (e.g., years 2023-2024)
2. Fire parallel `executeAsync()` calls
3. Gather results via `CompletableFuture.join()`
4. Merge and return

### UDT Handling
- **Ingestion**: `Map<String, Object>` → `UdtValue` (via session metadata)
- **Retrieval**: `UdtValue` → `Map<String, Object>` (recursive conversion)
- **Result**: Clean JSON with no Cassandra types

## 📚 Further Reading

- [Datastax Java Driver Docs](https://docs.datastax.com/en/developer/java-driver/4.17/)
- [QueryBuilder API](https://docs.datastax.com/en/developer/java-driver/4.17/manual/query_builder/)
- [Testcontainers Cassandra](https://www.testcontainers.org/modules/databases/cassandra/)

## 📄 License

MIT License - feel free to use this as a template for your own data platforms!

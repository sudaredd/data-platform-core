package com.platform.data.query.config;

import com.platform.data.common.config.TenantConfig;
import com.platform.data.common.registry.TenantConfigRegistry;
import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TenantInitializer {

    private static final Logger log = LoggerFactory.getLogger(TenantInitializer.class);

    @Autowired
    private TenantConfigRegistry registry;

    @PostConstruct
    public void init() {
        log.info("Initializing default tenant configurations...");

        TenantConfig config = TenantConfig.withBucket(
                "test_keyspace",
                "DailyNumeric",
                List.of("tenant_id", "instrument_id", "period_year"),
                "period_year",
                Set.of("data"));

        registry.register("IBM", "DAILY", "NUMERIC", config);
        registry.register("AAPL", "DAILY", "NUMERIC", config);
        registry.register("MSFT", "DAILY", "NUMERIC", config);
        registry.register("GOOGL", "DAILY", "NUMERIC", config);
        registry.register("AMZN", "DAILY", "NUMERIC", config);
        registry.register("NVDA", "DAILY", "NUMERIC", config);
        registry.register("META", "DAILY", "NUMERIC", config);
        registry.register("TSLA", "DAILY", "NUMERIC", config);

        log.info("Default tenants (IBM, MAG 7) registered for 'DAILY' 'NUMERIC' data.");
    }
}

package com.platform.data.query.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;

/**
 * Cassandra configuration for the query service.
 */
@Configuration
public class CassandraConfig {

    @Value("${cassandra.contact-points}")
    private String contactPoints;

    @Value("${cassandra.port}")
    private int port;

    @Value("${cassandra.local-datacenter}")
    private String localDatacenter;

    @Value("${cassandra.keyspace}")
    private String keyspace;

    @Bean
    public CqlSession cqlSession() {
        return new CqlSessionBuilder()
                .addContactPoint(new InetSocketAddress(contactPoints, port))
                .withLocalDatacenter(localDatacenter)
                .withKeyspace(keyspace)
                .build();
    }
}

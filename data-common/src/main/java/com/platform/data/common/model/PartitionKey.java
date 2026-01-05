package com.platform.data.common.model;

import java.util.List;

/**
 * Composite partition key for batch grouping.
 * Auto-implements equals/hashCode for use as Map keys.
 */
public record PartitionKey(List<Object> values) {

    /**
     * Creates a partition key from individual values.
     */
    public static PartitionKey of(Object... values) {
        return new PartitionKey(List.of(values));
    }

    @Override
    public String toString() {
        return "PartitionKey" + values;
    }
}

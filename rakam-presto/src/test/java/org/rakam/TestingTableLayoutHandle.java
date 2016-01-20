package org.rakam;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class TestingTableLayoutHandle implements ConnectorTableLayoutHandle  {
    private final TestingMetadata.InMemoryTableHandle table;
    private final TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public TestingTableLayoutHandle(
            @JsonProperty("table") TestingMetadata.InMemoryTableHandle table,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.table = requireNonNull(table, "table is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public TestingMetadata.InMemoryTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}

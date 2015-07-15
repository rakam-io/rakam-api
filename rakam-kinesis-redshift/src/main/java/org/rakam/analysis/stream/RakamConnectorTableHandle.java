package org.rakam.analysis.stream;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkSchemaName;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 12/07/15 06:06.
 */
public class RakamConnectorTableHandle implements ConnectorTableHandle {
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public RakamConnectorTableHandle(@JsonProperty("schemaName") String schemaName, @JsonProperty("tableName") String tableName)
    {
        this.schemaName = checkSchemaName(schemaName);
        this.tableName = checkTableName(tableName);
    }

    public RakamConnectorTableHandle(SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        this.schemaName = tableName.getSchemaName();
        this.tableName = tableName.getTableName();
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final RakamConnectorTableHandle other = (RakamConnectorTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }
}

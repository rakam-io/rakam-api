package org.rakam.presto.analysis.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.server.http.annotations.ApiParam;

public class CustomDataSource
{
    public final String schemaName;
    public final String type;
    public final Object options;

    @JsonCreator
    public CustomDataSource(
            @ApiParam("type") String type,
            @ApiParam(value = "schemaName") String schemaName,
            @ApiParam(value = "options") Object options)
    {
        this.schemaName = schemaName;
        this.options = DataSource.createDataSource(type, options);
        this.type = type;
    }
}

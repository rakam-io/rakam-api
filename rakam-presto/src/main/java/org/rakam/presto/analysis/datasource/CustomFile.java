package org.rakam.presto.analysis.datasource;

import com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource;
import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.server.http.annotations.ApiParam;

public class CustomFile
{
    public final String tableName;
    public final RemoteFileDataSource.RemoteFileSourceFactory options;

    @JsonCreator
    public CustomFile(
            @ApiParam("tableName") String tableName,
            @ApiParam("options") RemoteFileDataSource.RemoteFileSourceFactory options)
    {
        this.tableName = tableName;
        this.options = options;
    }
}

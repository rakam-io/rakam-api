package org.rakam.analysis.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.server.http.annotations.ApiParam;

import java.util.Locale;

public class CustomDataSource {
    public final String schemaName;
    public final String type;
    public final JDBCSchemaConfig options;

    @JsonCreator
    public CustomDataSource(
            @ApiParam("type") String type,
            @ApiParam(value = "schemaName") String schemaName,
            @ApiParam(value = "options") JDBCSchemaConfig options) {
        this.schemaName = schemaName.toLowerCase(Locale.ENGLISH);
        this.options = options;
        this.type = type;
    }
}

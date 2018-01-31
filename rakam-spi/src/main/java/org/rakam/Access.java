package org.rakam;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.server.http.annotations.ApiParam;

import java.util.List;

public final class Access {
    public final List<TableAccess> tableAccessList;

    @JsonCreator
    public Access(@ApiParam("tables") List<TableAccess> tableAccessList) {
        this.tableAccessList = tableAccessList;
    }

    public static class TableAccess {
        public final String tableName;
        public final String expression;

        @JsonCreator
        public TableAccess(@ApiParam("name") String tableName, @ApiParam("exp") String expression) {
            this.tableName = tableName;
            this.expression = expression;
        }
    }
}

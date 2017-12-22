package org.rakam.analysis.suggestion;


import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.*;
import org.rakam.util.NotExistsException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.analysis.suggestion.AttributeHook.TABLE_NAME;
import static org.rakam.util.ValidationUtil.*;

@Path("/suggestion")
@Api(value = "/suggestion", nickname = "query", description = "Auto-suggestion", tags = "query")
public class AutoSuggestionHttpService extends HttpService {

    private final QueryExecutorService queryExecutorService;
    private final AttributeHook hook;

    @Inject
    public AutoSuggestionHttpService(QueryExecutorService queryExecutorService, AttributeHook hook) {
        this.queryExecutorService = queryExecutorService;
        this.hook = hook;
    }


    @JsonRequest
    @ApiOperation(value = "Get possible attribute values",
            authorizations = @Authorization(value = "read_key"))
    @Path("/get")
    public CompletableFuture<List<String>> attributes(@Named("project") String project,
                                                      @ApiParam("collection") String collection,
                                                      @ApiParam("attribute") String attribute,
                                                      @ApiParam(value = "startDate", required = false) LocalDate startDate,
                                                      @ApiParam(value = "endDate", required = false) LocalDate endDate,
                                                      @ApiParam(value = "filter", required = false) String filter) {
        String query = String.format("SELECT value FROM materialized.%s WHERE collection = '%s' AND attribute = '%s'", TABLE_NAME,
                checkLiteral(collection), checkLiteral(attribute));

        if (startDate != null) {
            query += String.format(" AND date >= date '%s'", startDate.toString());
        }
        if (endDate != null) {
            query += String.format(" AND date <= date '%s'", endDate.toString());
        }
        if (filter != null && !filter.isEmpty()) {
            String value = "%" + filter.replaceAll("%", "\\%").replaceAll("_", "\\_") + "%";
            query += format(" AND value like '%s' escape '\\'", checkLiteral(value));
        }

        query += "GROUP BY 1 ORDER BY count(*) DESC LIMIT 25";

        QueryExecution queryExecution;
        try {
            queryExecution = queryExecutorService.executeQuery(project, query);
        } catch (NotExistsException e) {
            hook.add(project, collection);
            queryExecution = queryExecutorService.executeQuery(project, query);
        }
        return queryExecution.getResult().thenApply(value -> value.getResult().stream().map(e -> (String) e.get(0))
                        .collect(Collectors.toList()));
    }
}
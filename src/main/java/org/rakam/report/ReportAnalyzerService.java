package org.rakam.report;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.rakam.report.metadata.ReportMetadataStore;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.sql.SQLException;
import java.util.Map;

import static org.rakam.server.http.HttpServer.errorMessage;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 01:14.
 */
@Path("/reports")
public class ReportAnalyzerService implements HttpService {
    private final ReportMetadataStore database;
    private Map<String, ReportAnalyzer> jdbcDrivers = Maps.newConcurrentMap();

    @Inject
    public ReportAnalyzerService(ReportMetadataStore database) {
        this.database = database;
//        database.createReport("e74607921dad4803b998", "name0", "select 1");
//        database.createReport("e74607921dad4803b998", "name1", "select 1");
    }

    @JsonRequest
    @Path("/list")
    public Object list(JsonNode json) {
        JsonNode project = json.get("project");
        if (project == null) {
            return errorMessage("project parameter is required", 400);
        }

        return database.getReports(project.asText());
    }

    @JsonRequest
    @Path("/execute")
    public Object execute(JsonNode json) {
        JsonNode query = json.get("query");
        if (query == null || !query.isTextual()) {
            return errorMessage("query parameter is required", 400);
        }
        boolean columnar = JsonHelper.getOrDefault(json, "columnar", false);

        String addr = "jdbc:presto://127.0.0.1:8080";

        try {
            return jdbcDrivers.computeIfAbsent(addr, x -> new ReportAnalyzer(x)).execute(query.asText(), columnar);
        } catch (SQLException e) {
            throw new RakamException("Error executing sql query: "+e.getMessage(), 500);
        }
    }

    @GET
    @Path("/ff")
    public void ff(RakamHttpRequest request) {
        request.response("ff").end();
    }
}
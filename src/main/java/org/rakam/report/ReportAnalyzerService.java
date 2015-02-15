package org.rakam.report;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import org.rakam.report.metadata.ReportMetadataStore;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.ws.rs.Path;
import java.sql.Connection;
import java.sql.SQLException;

import static org.rakam.server.http.HttpServer.errorMessage;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 01:14.
 */
@Path("/reports")
public class ReportAnalyzerService implements HttpService {
    private final ReportMetadataStore database;
    private final JdbcPool jdbcPool;

    @Inject
    public ReportAnalyzerService(ReportMetadataStore database, JdbcPool jdbcPool) {
        this.database = database;
        this.jdbcPool = jdbcPool;
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
    @Path("/bind")
    public Object execute(JsonNode json) {
        JsonNode query = json.get("query");
        if (query == null || !query.isTextual()) {
            return errorMessage("query parameter is required", 400);
        }
        boolean columnar = JsonHelper.getOrDefault(json, "columnar", false);

        String addr = "jdbc:presto://127.0.0.1:8080";

        try {
            Connection conn = jdbcPool.getDriver(addr);
            return ReportAnalyzer.execute(conn, query.asText(), columnar);
        } catch (SQLException e) {
            throw new RakamException("Error executing sql query: "+e.getMessage(), 500);
        }
    }
}
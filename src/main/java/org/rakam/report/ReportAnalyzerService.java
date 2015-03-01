package org.rakam.report;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import org.rakam.analysis.Report;
import org.rakam.analysis.query.QueryFormatter;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
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
    private final EventSchemaMetastore metastore;
    private final SqlParser sqlParser;

    @Inject
    public ReportAnalyzerService(ReportMetadataStore database, EventSchemaMetastore metastore, JdbcPool jdbcPool) {
        this.database = database;
        this.jdbcPool = jdbcPool;
        this.metastore = metastore;
        this.sqlParser = new SqlParser();
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
    @Path("/add")
    public JsonNode add(Report report) {
        database.saveReport(report);
        return JsonHelper.jsonObject()
                .put("message", "Report successfully saved");
    }

    @JsonRequest
    @Path("/get")
    public Object get(JsonNode json) {
        JsonNode project = json.get("project");
        if (project == null) {
            return errorMessage("project parameter is required", 400);
        }
        JsonNode name = json.get("name");
        if(name == null) {
            return errorMessage("name parameter is required", 400);
        }

        return database.getReport(project.asText(), name.asText());
    }

    @JsonRequest
    @Path("/execute")
    public Object execute(JsonNode json) {
        JsonNode project = json.get("project");
        if (project == null || !project.isTextual())
            return errorMessage("project parameter is required", 400);

        JsonNode query = json.get("query");
        if (query == null || !query.isTextual())
            return errorMessage("query parameter is required", 400);

        boolean columnar = JsonHelper.getOrDefault(json, "columnar", false);

        String addr = "jdbc:presto://127.0.0.1:8080";

        Statement statement = null;
        try {
            statement = sqlParser.createStatement(query.asText());
        } catch (ParsingException e) {
            throw new RakamException("unable to parse query", 400);
        }
        StringBuilder builder = new StringBuilder();
        new QueryFormatter(builder, "kafka", project.asText()).process(statement, 0);

        try {
            Connection conn = jdbcPool.getDriver(addr);
            return ReportAnalyzer.execute(conn, builder.toString(), columnar);
        } catch (SQLException e) {
            throw new RakamException("Error executing sql query: "+e.getMessage(), 500);
        }
    }

    @JsonRequest
    @Path("/explain")
    public Object explain(JsonNode json) {
        JsonNode project = json.get("project");

        if (project == null || !project.isTextual()) {
            return errorMessage("project parameter is required", 400);
        }

        return metastore.getSchemas(project.asText());//.entrySet()
                //.stream()
                //.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toString()));
    }
}
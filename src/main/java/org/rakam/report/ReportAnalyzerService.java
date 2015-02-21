package org.rakam.report;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.IdentifierSymbol;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.Statement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
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
import java.util.EnumSet;

import static org.rakam.server.http.HttpServer.errorMessage;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 01:14.
 */
@Path("/reports")
public class ReportAnalyzerService implements HttpService {
    private final ReportMetadataStore database;
    private final JdbcPool jdbcPool;
    private final EventSchemaMetastore metastore;

    @Inject
    public ReportAnalyzerService(ReportMetadataStore database, EventSchemaMetastore metastore, JdbcPool jdbcPool) {
        this.database = database;
        this.jdbcPool = jdbcPool;
        this.metastore = metastore;
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
    @Path("/analyze_query")
    public Object analyze_query(JsonNode json) {
        JsonNode query = json.get("query");
        if (query == null) {
            return errorMessage("query parameter is required", 400);
        }

        Statement statement = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(EnumSet.allOf(IdentifierSymbol.class))).createStatement(query.asText());
        SqlFormatter.formatSql(statement);

        StringBuilder builder = new StringBuilder();
        new QueryFormatter(builder, "", "").process(statement, 0);
        return TextNode.valueOf(builder.toString());
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
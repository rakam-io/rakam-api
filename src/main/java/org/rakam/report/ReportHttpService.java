package org.rakam.report;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.MaterializedView;
import org.rakam.analysis.Report;
import org.rakam.analysis.query.QueryFormatter;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.report.metadata.ReportMetadataStore;
import org.rakam.server.http.ForHttpServer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.encode;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 01:14.
 */
@Path("/reports")
public class ReportHttpService implements HttpService {
    private final ReportMetadataStore database;
    private final EventSchemaMetastore metastore;
    private final SqlParser sqlParser;
    private final PrestoQueryExecutor queryExecutor;
    private final PrestoConfig prestoConfig;
    private EventLoopGroup eventLoopGroup;

    @Inject
    public ReportHttpService(ReportMetadataStore database, EventSchemaMetastore metastore, PrestoConfig prestoConfig, PrestoQueryExecutor queryExecutor) {
        this.database = database;
        this.metastore = metastore;
        this.prestoConfig = prestoConfig;
        this.queryExecutor = queryExecutor;
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
    @Path("/add/report")
    public JsonNode addReport(Report report) {
        database.saveReport(report);
        return JsonHelper.jsonObject()
                .put("message", "Report successfully saved");
    }

    @JsonRequest
    @Path("/add/materialized-view")
    public JsonNode addMaterializedView(MaterializedView report) {
        database.createMaterializedView(report);
        return JsonHelper.jsonObject()
                .put("message", "Materialized view successfully created");
    }

    @JsonRequest
    @Path("/get")
    public Object get(JsonNode json) {
        JsonNode project = json.get("project");
        if (project == null) {
            return errorMessage("project parameter is required", 400);
        }
        JsonNode name = json.get("name");
        if (name == null) {
            return errorMessage("name parameter is required", 400);
        }

        return database.getReport(project.asText(), name.asText());
    }

    @GET
    @Path("/execute")
    public void execute(RakamHttpRequest request) {
        if (!Objects.equals(request.headers().get(HttpHeaders.Names.ACCEPT), "text/event-stream")) {
            request.response("the response should accept text/event-stream", HttpResponseStatus.NOT_ACCEPTABLE).end();
            return;
        }

        RakamHttpRequest.StreamResponse response = request.streamResponse();
        List<String> data = request.params().get("data");
        if (data == null || data.isEmpty()) {
            response.send("result", encode(errorMessage("data query parameter is required", 400))).end();
            return;
        }

        ObjectNode json;
        try {
            json = JsonHelper.readSafe(data.get(0));
        } catch (IOException e) {
            response.send("result", encode(errorMessage("json couldn't parsed", 400))).end();
            return;
        }

        JsonNode project = json.get("project");
        if (project == null || !project.isTextual()) {
            response.send("result", encode(errorMessage("project parameter is required", 400))).end();
            return;
        }

        JsonNode query = json.get("query");
        if (project == null || !project.isTextual()) {
            response.send("result", encode(errorMessage("query parameter is required", 400))).end();
            return;
        }

        Query statement;
        try {
            statement = (Query) sqlParser.createStatement(query.asText());
        } catch (ParsingException e) {
            response.send("result", encode(errorMessage("unable to parse query", 400))).end();
            return;
        }

        StringBuilder builder = new StringBuilder();
        // TODO: does cold storage supports schemas?
        new QueryFormatter(builder, node -> {
            QualifiedName prefix = node.getName().getPrefix().orElse(new QualifiedName(prestoConfig.getColdStorageConnector()));
            return prefix.getSuffix() + "." + project + "." + node.getName().getSuffix();
        }).process(statement, 0);

        String sqlQuery = builder.toString();

        PrestoQuery prestoQuery = queryExecutor.executeQuery(sqlQuery);
        handleQueryExecution(eventLoopGroup, response, prestoQuery);
    }

    public static void handleQueryExecution(EventLoopGroup eventLoopGroup, RakamHttpRequest.StreamResponse response, PrestoQuery prestoQuery) {
        eventLoopGroup.schedule(new Runnable() {
            @Override
            public void run() {
                if(prestoQuery.isFinished()) {
                    QueryResult result = prestoQuery.getResult().join();
                    if(result.isFailed()) {
                        response.send("result", JsonHelper.jsonObject()
                                .put("success", false)
                                .put("query", prestoQuery.getQuery())
                                .put("message", result.getError().message)).end();
                    }else {
                        response.send("result", encode(JsonHelper.jsonObject()
                                .put("success", true)
                                .putPOJO("query", prestoQuery.getQuery())
                                .putPOJO("result", result.getResult())
                                .putPOJO("metadata", result.getMetadata()))).end();
                    }
                }else {
                    response.send("stats", encode(prestoQuery.currentStats()));
                    eventLoopGroup.schedule(this, 500, TimeUnit.MILLISECONDS);
                }
            }
        }, 500, TimeUnit.MILLISECONDS);
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

    @Inject
    public void setWorkerGroup(@ForHttpServer EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }

}
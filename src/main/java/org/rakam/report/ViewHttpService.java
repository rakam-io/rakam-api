package org.rakam.report;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import io.netty.channel.EventLoopGroup;
import org.rakam.analysis.ContinuousQuery;
import org.rakam.analysis.query.QueryFormatter;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.report.metadata.ReportMetadataStore;
import org.rakam.server.http.ForHttpServer;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;

import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static org.rakam.report.ReportHttpService.handleQueryExecution;
import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.encode;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/03/15 17:35.
 */
@Path("/reports")
public class ViewHttpService {
    private final ReportMetadataStore database;
    private final EventSchemaMetastore metastore;
    private final SqlParser sqlParser;
    private final PrestoConfig prestoConfig;
    private final PrestoQueryExecutor queryExecutor;
    private EventLoopGroup eventLoopGroup;

    @Inject
    public ViewHttpService(ReportMetadataStore database, EventSchemaMetastore metastore, PrestoConfig prestoConfig, PrestoQueryExecutor queryExecutor) {
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

    @Path("/create")
    public void create(RakamHttpRequest request) {
        RakamHttpRequest.StreamResponse response = request.streamResponse();

        List<String> data = request.params().get("data");
        if (data == null || data.isEmpty()) {
            response.send("result", encode(errorMessage("data query parameter is required", 400))).end();
            return;
        }

        ContinuousQuery view;
        try {
            view = JsonHelper.readSafe(data.get(0), ContinuousQuery.class);
        } catch (IOException e) {
            response.send("result", encode(errorMessage("json couldn't parsed", 400))).end();
            return;
        }

        Query statement;
        try {
            statement = (Query) sqlParser.createStatement(view.query);
        } catch (ParsingException e) {
            request.jsonResponse(errorMessage("unable to parse query", 400)).end();
            return;
        }

        StringBuilder builder = new StringBuilder();

        switch (view.strategy) {
            case STREAM:
                Optional<Relation> from = ((QuerySpecification) statement.getQueryBody()).getFrom();
                if(!from.isPresent() || !(from.get() instanceof Table)) {
                    request.jsonResponse(errorMessage("stream queries must reference a stream table. (FROM collectionName)", 400)).end();
                    return;
                }
                Table relation = (Table) from.get();

                new QueryFormatter(builder, node -> {
                    String connector = node == relation ? prestoConfig.getHotStorageConnector() : prestoConfig.getColdStorageConnector();
                    QualifiedName prefix = node.getName().getPrefix().orElse(new QualifiedName(connector));
                    return prefix.getSuffix() + "." + view.project + "." + node.getName().getSuffix();
                }).process(statement, 0);
                break;
            case INCREMENTAL:
                new QueryFormatter(builder, node -> {
                    QualifiedName prefix = node.getName().getPrefix().orElse(new QualifiedName(prestoConfig.getColdStorageConnector()));
                    return prefix.getSuffix() + "." + view.project + "." + node.getName().getSuffix();
                }).process(statement, 0);
                break;
            default:
                throw new IllegalStateException();
        }

        String query = format("create table %s.%s.%s as (%s)",
                prestoConfig.getColdStorageConnector(),
                "default",
                view.name,
                builder.toString());

        database.createContinuousQuery(view);

        PrestoQuery prestoQuery = queryExecutor.executeQuery(query);
        handleQueryExecution(eventLoopGroup, response, prestoQuery);
    }

    @Inject
    public void setWorkerGroup(@ForHttpServer EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }
}

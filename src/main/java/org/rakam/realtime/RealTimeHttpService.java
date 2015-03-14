package org.rakam.realtime;

import com.facebook.presto.jdbc.internal.guava.collect.ImmutableMap;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Singleton;
import org.rakam.analysis.MaterializedView;
import org.rakam.analysis.TableStrategy;
import org.rakam.report.JdbcPool;
import org.rakam.report.ReportAnalyzer;
import org.rakam.report.metadata.ReportMetadataStore;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;

import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.rakam.server.http.HttpServer.errorMessage;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 14:30.
 */
@Singleton
@Path("/realtime")
public class RealTimeHttpService implements HttpService {

    private final JdbcPool jdbcPool;
    private final ReportMetadataStore metastore;
    String addr = "jdbc:presto://127.0.0.1:8080";

    @Inject
    public RealTimeHttpService(ReportMetadataStore metastore, JdbcPool jdbcPool) {
        this.jdbcPool = checkNotNull(jdbcPool, "jdbcPool is null");
        this.metastore = checkNotNull(metastore, "metastore is null");
    }

    @JsonRequest
    @POST
    @Path("/add")
    public JsonNode add(RealTimeReport query) {
        ObjectNode options = JsonHelper.jsonObject()
                .put("type", "realtime")
                .put("report", JsonHelper.encode(query));
        MaterializedView report = new MaterializedView(query.project, query.name, buildQuery(query), TableStrategy.STREAM, Instant.now(), query.collections, null);
        metastore.createMaterializedView(report);
        return JsonHelper.jsonObject().
                put("message", "report successfully added");
    }

    @JsonRequest
    @POST
    @Path("/list")
    public Object list(JsonNode json) {
        JsonNode project = json.get("project");
        if (project == null) {
            return errorMessage("project parameter is required", 400);
        }
        return metastore.getReports(project.asText()).stream()
                .filter(report -> Objects.equals(report.options.get("type"), "realtime"))
                .map(report -> JsonHelper.read(report.options.get("report").asText(), RealTimeReport.class))
                .collect(Collectors.toList());
    }

    @JsonRequest
    @POST
    @Path("/remove")
    public Object remove(JsonNode json) {
        JsonNode project = json.get("project");
        if (project == null || !project.isTextual()) {
            return errorMessage("project parameter is required", 400);
        }
        JsonNode name = json.get("name");
        if (name == null || !name.isTextual()) {
            return errorMessage("name parameter is required", 400);
        }

        // TODO: Check if it's a real-time report.
        metastore.deleteReport(project.asText(), name.asText());

        return JsonHelper.jsonObject().put("message", "successfully deleted");
    }

    @JsonRequest
    @POST
    @Path("/execute")
    public JsonNode execute(RealTimeReport query) {
        Connection driver = jdbcPool.getDriver(addr);

        String sqlQuery = buildQuery(query);
        try {
            ObjectNode result = ReportAnalyzer.execute(driver, sqlQuery, false);
            if (query.dimension == null) {
                return result.get("result").get(0);
            } else {
                result.remove("metadata");
                JsonNode resultNode = result.remove("result");
                result.set("value", resultNode);
                return result;
            }
        } catch (SQLException e) {
            return errorMessage(format("error while executing query (%s): %s", sqlQuery, e.getCause().getMessage()), 500);
        }
    }

    public String buildQuery(RealTimeReport query) throws NotFoundException {
        Map<String, Long> offsetOfCollections;

        if (query.collections == null || query.collections.isEmpty()) {
//            offsetOfCollections = kafkaManager.getOffsetOfCollections(query.project);
        } else {
            ImmutableMap.Builder<String, Long> builder = ImmutableMap.<String, Long>builder();
            for (String collection : query.collections) {
//                Long offsetOfCollection = kafkaManager.getOffsetOfCollection(query.project, collection);
//                if(offsetOfCollection == null) {
//                    throw new NotFoundException(format("couldn't found collection %s", collection));
//                }
//                builder.put(collection, offsetOfCollection);
            }
            offsetOfCollections = builder.build();
        }

//        if (offsetOfCollections.isEmpty()) {
//            throw new NotFoundException("couldn't found any collection");
//        }

        String column;

        if(query.measure == null || query.measure.equals("*")) {
            column =  query.dimension == null ? "count(*)" : query.dimension;
        }else {
            if(query.dimension == null || query.dimension.trim().equals(query.measure.trim())) {
                column =  query.measure;
            } else {
                column =  query.dimension + ", "+ query.measure;
            }
        }

        StringBuilder builder = new StringBuilder();

        builder.append("select ")
                .append(createSelect(query.aggregation, query.measure, query.dimension))
                .append(" from (");

//        String[] subQueries = offsetOfCollections.entrySet().stream().map(entry -> {
//            String collection = entry.getKey().split("_", 2)[0];
//            return format(" (select %s as key from %s where %s) ",
//                    column,
//                    createFrom(collection),
//                    createWhere(entry.getValue(), query.filter));
//                    createWhere(0, query.filter));
//        }).toArray(String[]::new);

//        builder.append(Joiner.on(" union ").join(subQueries))
//                .append(") ").append(createGroupBy(query.dimension));

        builder.append(" order by value desc");

        return builder.toString();
    }

    public String createSelect(AggregationType aggType, String measure, String dimension) {

        if (measure == null) {
            if (aggType != AggregationType.COUNT)
                throw new IllegalArgumentException("either measure.expression or measure.field must be specified.");
        }

        StringBuilder builder = new StringBuilder();
        if (dimension != null)
            builder.append(" key, ");

        switch (aggType) {
            case AVERAGE:
                return builder.append("avg(key) as value").toString();
            case MAXIMUM:
                return builder.append("max(key) as value").toString();
            case MINIMUM:
                return builder.append("min(key) as value").toString();
            case COUNT_UNIQUE:
                return builder.append("count(distinct key) as value").toString();
            case COUNT:
                return builder.append("count(key) as value").toString();
            case SUM:
                return builder.append("sum(key) as value").toString();
            case APPROXIMATE_UNIQUE:
                return builder.append("approx_distinct(key) as value").toString();
            case VARIANCE:
                return builder.append("variance(key) as value").toString();
            case POPULATION_VARIANCE:
                return builder.append("variance(key) as value").toString();
            case STANDARD_DEVIATION:
                return builder.append("stddev(key) as value").toString();
            default:
                throw new IllegalArgumentException("aggregation type couldn't found.");
        }
    }

    public String createFrom(String collection) {
        return format("stream.%s", collection);
    }

    public String createWhere(long offset, String filter) {
        return format("_offset > %s %s", offset, filter == null ? "" : "AND " + filter);
    }

    public String createGroupBy(String dimension) {
        return dimension != null ? "group by 1" : "";
    }
}

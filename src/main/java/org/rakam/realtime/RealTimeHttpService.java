package org.rakam.realtime;

import com.facebook.presto.jdbc.internal.guava.collect.ImmutableMap;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.inject.Singleton;
import org.rakam.realtime.RealTimeReport.RealTimeQueryField;
import org.rakam.realtime.metadata.RealtimeReportMetadataStore;
import org.rakam.report.JdbcPool;
import org.rakam.report.ReportAnalyzer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.rakam.server.http.HttpServer.errorMessage;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 14:30.
 */
@Singleton
@Path("/realtime")
public class RealTimeHttpService implements HttpService {

    private final KafkaOffsetManager kafkaManager;
    private final JdbcPool jdbcPool;
    private final RealtimeReportMetadataStore metastore;
    String addr = "jdbc:presto://127.0.0.1:8080";

    @Inject
    public RealTimeHttpService(KafkaOffsetManager kafkaManager, RealtimeReportMetadataStore metastore, JdbcPool jdbcPool) {
        this.kafkaManager = checkNotNull(kafkaManager, "kafkaManager is null");
        this.jdbcPool = checkNotNull(jdbcPool, "jdbcPool is null");
        this.metastore = checkNotNull(metastore, "metastore is null");
    }

    @JsonRequest
    @POST
    @Path("/add")
    public JsonNode add(RealTimeReport query) {
        metastore.saveReport(query);
        return JsonHelper.jsonObject().
                put("message", "report successfully added");
    }

    @JsonRequest
    @POST
    @Path("/list")
    public Object add(JsonNode json) {
        JsonNode project = json.get("project");
        if (project == null) {
            return errorMessage("project parameter is required", 400);
        }

        return metastore.getReports(project.asText());
    }

    @JsonRequest
    @POST
    @Path("/execute")
    public JsonNode execute(RealTimeReport query) {
        String sqlQuery;

        Connection driver = jdbcPool.getDriver(addr);

        Map<String, Long> offsetOfCollections;

        if (query.collections == null || query.collections.isEmpty()) {
            offsetOfCollections = kafkaManager.getOffsetOfCollections(query.project);
        } else {
            ImmutableMap.Builder<String, Long> builder = ImmutableMap.<String, Long>builder();
            for (String collection : query.collections) {
                Long offsetOfCollection = kafkaManager.getOffsetOfCollection(query.project, collection);
                if(offsetOfCollection == null) {
                    return JsonHelper.jsonObject().put("error", format("couldn't found collection %s", collection));
                }
                builder.put(collection, offsetOfCollection);
            }
            offsetOfCollections = builder.build();
        }

        if (offsetOfCollections.isEmpty()) {
            return JsonHelper.jsonObject().put("error", "couldn't found any collection");
        }

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

        String[] subQueries = offsetOfCollections.entrySet().stream().map(entry -> {
            String collection = entry.getKey().split("_", 2)[0];
            return format(" (select %s as key from %s where %s) ",
                    column,
                    createFrom(query.project, collection),
//                    createWhere(entry.getValue(), query.filter));
                    createWhere(0, query.filter));
        }).toArray(String[]::new);

        builder.append(Joiner.on(" union ").join(subQueries))
                .append(") ").append(createGroupBy(query.dimension));

        builder.append(" order by value desc");

        sqlQuery = builder.toString();

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
            return errorMessage("error while executing query: " + e.getMessage(), 500);
        }
    }

    public String createSelect(AggregationType aggType, String measure, String dimension) {

        String field;
        if (measure == null) {
            if (aggType != AggregationType.COUNT)
                throw new IllegalArgumentException("either measure.expression or measure.field must be specified.");

            field = "*";
        } else {
            field = measure;
        }

        StringBuilder builder = new StringBuilder();
        if (dimension != null)
            builder.append(" key, ");

        switch (aggType) {
            case AVERAGE:
                return builder.append(format("avg(%s) as value", field)).toString();
            case MAXIMUM:
                return builder.append(format("max(%s) as value", field)).toString();
            case MINIMUM:
                return builder.append(format("min(%s) as value", field)).toString();
            case COUNT_UNIQUE:
                return builder.append(format("count(distinct %s) as value", field)).toString();
            case COUNT:
                return builder.append(format("count(%s) as value", field)).toString();
            case SUM:
                return builder.append(format("sum(%s) as value", field)).toString();
            case APPROXIMATE_UNIQUE:
                return builder.append(format("approx_distinct(%s) as value", field)).toString();
            case VARIANCE:
                return builder.append(format("variance(%s) as value", field)).toString();
            case POPULATION_VARIANCE:
                return builder.append(format("variance(%s) as value", field)).toString();
            case STANDARD_DEVIATION:
                return builder.append(format("stddev(%s) as value", field)).toString();
            default:
                throw new IllegalArgumentException("aggregation type couldn't found.");
        }
    }

    public String createFrom(String project, String collection) {
        return format("kafka.%s.%s", project, collection);
    }

    public String createWhere(long offset, String filter) {
        return format("_offset > %s %s", offset, filter == null ? "" : "AND " + filter);
    }

    public String createGroupBy(String dimension) {
        return dimension != null ? "group by 1" : "";
    }

    private String getFieldValue(RealTimeQueryField field, String fieldName) {
        if (field == null)
            return null;

        if (field.expression == null && field.field == null)
            throw new IllegalArgumentException(format("either %s.expression or %s.field must be specified.", fieldName));

        if (field.expression != null && field.field != null)
            throw new IllegalArgumentException(format("only one of %s.expression and %s.field must be specified.", fieldName));

        return field.field != null ? field.field : field.expression;
    }
}

package org.rakam.realtime;

import com.facebook.presto.jdbc.internal.guava.collect.ImmutableMap;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.inject.Singleton;
import org.rakam.realtime.RealTimeRequest.RealTimeQueryField;
import org.rakam.report.JdbcPool;
import org.rakam.report.ReportAnalyzer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.JsonRequest;

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
    String addr = "jdbc:presto://127.0.0.1:8080";

    @Inject
    public RealTimeHttpService(KafkaOffsetManager kafkaManager, JdbcPool jdbcPool) {
        this.kafkaManager = checkNotNull(kafkaManager, "kafkaManager is null");
        this.jdbcPool = checkNotNull(jdbcPool, "jdbcPool is null");
    }

    @JsonRequest
    @POST
    @Path("/bind")
    public JsonNode execute(RealTimeRequest query) {
        String sqlQuery;

        Connection driver = jdbcPool.getDriver(addr);

        Map<String, Long> offsetOfCollections;

        if (query.collection == null) {
            offsetOfCollections = kafkaManager.getOffsetOfCollections(query.project);
        } else {
            long offsetOfCollection = kafkaManager.getOffsetOfCollection(query.project, query.collection);
            offsetOfCollections = ImmutableMap.<String, Long>builder().put(query.collection, offsetOfCollection).build();
        }

        String valColumn = getFieldValue(query.measure, "measure");
        String keyColumn = getFieldValue(query.dimension, "dimension");
        String column = (keyColumn == null ? "" : keyColumn + " , ") + (valColumn != null ? valColumn : "*");

        StringBuilder builder = new StringBuilder();

        builder.append("select ")
                .append(createSelect(query.aggregation, query.measure, query.dimension))
                .append(" from (");

        String[] subQueries = offsetOfCollections.entrySet().stream().map(entry -> {
            String collection = entry.getKey().split("_", 2)[0];
            return format(" (select %s from %s where %s) ",
                    column,
                    createFrom(query.project, collection),
                    createWhere(entry.getValue(), query.filter));
        }).toArray(String[]::new);

        builder.append(Joiner.on(" union ").join(subQueries))
                .append(") ").append(createGroupBy(query.dimension));

        sqlQuery = builder.toString();

        try {
            ObjectNode result = ReportAnalyzer.execute(driver, sqlQuery, false);
            if(query.dimension == null) {
                return result.get("result").get(0);
            }else {
                result.remove("metadata");
                return result;
            }
        } catch (SQLException e) {
            return errorMessage("error while executing query: " + e.getMessage(), 500);
        }
    }

    public String createSelect(AggregationType aggType, RealTimeQueryField measure, RealTimeQueryField dimension) {

        String field;
        if (measure == null) {
            if (aggType != AggregationType.COUNT)
                throw new IllegalArgumentException("either measure.expression or measure.field must be specified.");

            field = "*";
        } else {
            field = getFieldValue(measure, "measure");
        }

        StringBuilder builder = new StringBuilder();
        if (dimension != null)
            builder.append(dimension.field != null ? dimension.field : dimension.expression).append(" as key, ");

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
            case POPULATION_STANDARD_DEVIATION:
                return builder.append(format("stddev_pop(%s) as value", field)).toString();
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

    public String createGroupBy(RealTimeQueryField dimension) {
        String value = getFieldValue(dimension, "dimension");
        return value != null ? "group by " + value : "";
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

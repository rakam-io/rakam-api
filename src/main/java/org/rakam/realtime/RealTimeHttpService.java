package org.rakam.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.inject.Singleton;
import org.rakam.report.JdbcPool;
import org.rakam.report.ReportAnalyzer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.NotExistsException;

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
    public JsonNode execute(RealtimeRequest query) {
        String sqlQuery;

        if (query.collection == null) {
            Map<String, Long> offsetOfCollections = kafkaManager.getOffsetOfCollections(query.project);

            String column = Objects.firstNonNull(query.measure.field, query.measure.expression);
            StringBuilder builder = new StringBuilder();

            builder.append("select ")
                    .append(createSelect(query.aggregation, query.measure))
                    .append(" from (");

            String[] subQueries = offsetOfCollections.entrySet().stream().map(entry -> {
                String collection = entry.getKey().split("_", 2)[0];
                return format(" (select %s from %s where %s) ",
                        column,
                        createFrom(query.project, collection),
                        createWhere(entry.getValue(), query.filter));
            }).toArray(String[]::new);

            builder.append(Joiner.on(" union ").join(subQueries))
                    .append(")").append(createGroupBy(query.dimension));

            sqlQuery = builder.toString();
        } else {
            long offsetOfCollection;

            try {
                offsetOfCollection = kafkaManager.getOffsetOfCollection(query.project, query.collection);
            } catch (NotExistsException e) {
                return errorMessage(e.getMessage(), 500);
            }

            sqlQuery = format("select %s from %s where %s %s",
                    createSelect(query.aggregation, query.measure),
                    createFrom(query.project, query.collection),
                    createWhere(offsetOfCollection, query.filter),
                    createGroupBy(query.dimension));
        }
        Connection driver = jdbcPool.getDriver(addr);

        try {
            return ReportAnalyzer.execute(driver, sqlQuery, false);
        } catch (SQLException e) {
            return errorMessage("error while executing query: " + e.getMessage(), 500);
        }
    }

    public String createSelect(AggregationType aggType, RealtimeRequest.RealtimeQueryField queryField) {

        String field;
        if (queryField.expression == null && queryField.field == null) {
            if (aggType != AggregationType.COUNT)
                throw new IllegalArgumentException("either field.expression or field.field must be specified.");

            field = "*";
        } else if (queryField.expression != null && queryField.field != null) {
            throw new IllegalArgumentException("only one of field.expression and field.field must be specified.");
        } else {
            field = queryField.expression != null ? queryField.expression : queryField.field;
        }

        switch (aggType) {
            case AVERAGE:
                return format("avg(%s)", field);
            case MAXIMUM:
                return format("max(%s)", field);
            case MINIMUM:
                return format("min(%s)", field);
            case COUNT_UNIQUE:
                return format("count(distinct %s)", field);
            case COUNT:
                return format("count(%s)", field);
            case SUM:
                return format("sum(%s)", field);
            case APPROXIMATE_UNIQUE:
                return format("approx_distinct(%s)", field);
            case VARIANCE:
                return format("variance(%s)", field);
            case POPULATION_VARIANCE:
                return format("variance(%s)", field);
            case POPULATION_STANDARD_DEVIATION:
                return format("stddev_pop(%s)", field);
            case STANDARD_DEVIATION:
                return format("stddev(%s)", field);
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

    public String createGroupBy(RealtimeRequest.RealtimeQueryField dimension) {
        if (dimension == null)
            return "";

        if (dimension.expression == null && dimension.field == null)
            throw new IllegalArgumentException("either dimension.expression or dimension.field must be specified.");

        if (dimension.expression != null && dimension.field != null)
            throw new IllegalArgumentException("only one of dimension.expression and dimension.field must be specified.");


        return "group by " + (dimension.expression != null ? dimension.expression : dimension.field);
    }

}

package org.rakam.analysis;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import org.rakam.analysis.query.simple.SimpleFieldScript;
import org.rakam.analysis.rule.aggregation.AggregationReport;
import org.rakam.constant.AggregationType;
import org.rakam.server.RouteMatcher;
import org.rakam.server.http.HttpService;
import org.rakam.util.Interval;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import java.sql.SQLException;
import java.util.HashMap;

import static org.rakam.server.WebServer.errorMessage;
import static org.rakam.server.WebServer.mapJsonRequest;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 01:14.
 */
public class ReportAnalyzerService implements HttpService {
    private final DefaultReportDatabase database;
    private HashMap<String, ReportAnalyzer> jdbcDrivers;

    @Inject
    public ReportAnalyzerService(DefaultReportDatabase database) {
        this.database = database;
        database.add(new AggregationReport("e74607921dad4803b998", "name", AggregationType.COUNT, null, null, null, Interval.DAY));
        database.add(new AggregationReport("e74607921dad4803b998", "name", AggregationType.AVERAGE_X, null, null, new SimpleFieldScript<>("test"), Interval.DAY));
    }

    @Override
    public String getEndPoint() {
        return "/reports";
    }

    @Override
    public void register(RouteMatcher.MicroRouteMatcher routeMatcher) {
        mapJsonRequest(routeMatcher, "/list", json -> {
            JsonNode project = json.get("project");
            if (project == null) {
                return errorMessage("project parameter is required", 400);
            }

            return database.get(project.asText());
        });
        mapJsonRequest(routeMatcher, "/execute", json -> {
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
        });
    }
}
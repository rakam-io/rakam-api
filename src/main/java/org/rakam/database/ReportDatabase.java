package org.rakam.database;

import org.rakam.analysis.rule.aggregation.AggregationReport;

import java.util.Map;
import java.util.Set;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 05:21.
 */
public interface ReportDatabase {
    Map<String, Set<AggregationReport>> getAllReports();

    void add(AggregationReport rule);

    void delete(AggregationReport rule);

    Set<AggregationReport> get(String project);

    void clear();
}

package org.rakam.analysis.rule.aggregation;

import org.rakam.constant.AggregationAnalysis;
import org.rakam.constant.Analysis;
import org.rakam.constant.AnalysisRuleStrategy;
import org.rakam.util.json.JsonObject;

/**
 * Created by buremba on 05/05/14.
 */
public abstract class AnalysisRule {
    public String project;
    public AnalysisRuleStrategy strategy = AnalysisRuleStrategy.REAL_TIME;
    public boolean batch_status = false;

    public abstract Analysis analysisType();

    public abstract JsonObject toJson();

    private String id;

    public String id() {
        if (id == null) {
            final JsonObject jsonObject = toJson();
            jsonObject.removeField("strategy");
            id = jsonObject.encode();
        }
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AnalysisRule)) return false;

        AnalysisRule that = (AnalysisRule) o;

        if (!project.equals(that.project)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = project.hashCode();
        result = 31 * result + strategy.hashCode();
        return result;
    }

    public abstract boolean canAnalyze(AggregationAnalysis rule);

}

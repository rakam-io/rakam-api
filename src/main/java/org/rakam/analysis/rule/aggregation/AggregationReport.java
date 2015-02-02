package org.rakam.analysis.rule.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.analysis.query.FieldScript;
import org.rakam.analysis.query.FilterScript;
import org.rakam.constant.AggregationAnalysis;
import org.rakam.constant.AggregationType;
import org.rakam.constant.AnalysisRuleStrategy;
import org.rakam.util.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Created by buremba on 16/01/14.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AggregationReport {
    public final String project;
    public AnalysisRuleStrategy strategy = AnalysisRuleStrategy.REAL_TIME;
    public  final AggregationType type;
    public final String name;

    public final @Nullable FieldScript<String> groupBy;
    public final @Nullable FilterScript filter;
    public final @Nullable FieldScript<String> select;

    public Interval interval;

    @JsonCreator
    public AggregationReport(
            @JsonProperty("project") String project,
            @JsonProperty("name") String name,
            @JsonProperty("type") AggregationType type,
            @JsonProperty("groupBy") FieldScript<String> groupBy,
            @JsonProperty("filter") FilterScript filter,
            @JsonProperty("select") FieldScript<String> select,
            @JsonProperty("interval") Interval interval) {
        if (type != AggregationType.COUNT && select == null) {
            throw new IllegalArgumentException("select parameter must be provided.");
        }
        this.project = project;
        this.name = name;
        this.groupBy = groupBy;
        this.filter = filter;
        this.type = type;
        this.select = select;
        this.interval = interval;
    }

    public boolean canAnalyze(AggregationAnalysis analysis) {
        return Arrays.asList(analysis.getAnalyzableAggregationTypes()).contains(type);
    }

    public boolean isMultipleInterval(AggregationReport rule) {
        return rule.project.equals(project) &&
                rule.type.equals(type) && Objects.equals(rule.select, select) &&
                Objects.equals(rule.filter, filter) && Objects.equals(rule.groupBy, groupBy)
                && interval.isDivisible(rule.interval);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AggregationReport)) return false;

        AggregationReport that = (AggregationReport) o;

        if (filter != null ? !filter.equals(that.filter) : that.filter != null) return false;
        if (groupBy != null ? !groupBy.equals(that.groupBy) : that.groupBy != null) return false;
        if (interval != null ? !interval.equals(that.interval) : that.interval != null) return false;
        if (!name.equals(that.name)) return false;
        if (!project.equals(that.project)) return false;
        if (select != null ? !select.equals(that.select) : that.select != null) return false;
        if (strategy != that.strategy) return false;
        if (type != that.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = project.hashCode();
        result = 31 * result + strategy.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + (groupBy != null ? groupBy.hashCode() : 0);
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (select != null ? select.hashCode() : 0);
        result = 31 * result + (interval != null ? interval.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AggregationReport{" +
                "project='" + project + '\'' +
                ", strategy=" + strategy +
                ", name='" + name + '\'' +
                ", groupBy=" + groupBy +
                ", filter=" + filter +
                ", type=" + type +
                ", select=" + select +
                ", interval=" + interval +
                '}';
    }
}

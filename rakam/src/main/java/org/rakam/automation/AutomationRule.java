package org.rakam.automation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.Event;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.RakamException;

import java.util.List;
import java.util.function.Predicate;

public class AutomationRule {
    public final int id;
    public final List<ScenarioStep> scenarios;
    public final List<SerializableAction> actions;
    @JsonProperty("custom_data")
    public final String customData;
    @JsonProperty("is_active")
    public boolean isActive;

    @JsonCreator
    public AutomationRule(@ApiParam(value = "is_active", required = false) Boolean isActive,
                          @ApiParam("scenarios") List<ScenarioStep> scenarios,
                          @ApiParam("actions") List<SerializableAction> actions,
                          @ApiParam(value = "custom_data", required = false) String customData) {
        this.id = -1;
        this.customData = customData;
        this.isActive = isActive == null ? true : isActive.booleanValue();
        this.scenarios = scenarios;
        this.actions = actions;
    }

    public AutomationRule(int id, boolean isActive, List<ScenarioStep> scenarios, List<SerializableAction> actions, String customData) {
        this.id = id;
        this.isActive = isActive;
        this.scenarios = scenarios;
        this.actions = actions;
        this.customData = customData;
    }

    public synchronized void setActive(boolean active) {
        this.isActive = active;
    }

    public enum ThresholdAggregation {
        count(false), sum(true);

        private final boolean fieldRequired;

        ThresholdAggregation(boolean fieldRequired) {
            this.fieldRequired = fieldRequired;
        }

        public boolean isFieldRequired() {
            return fieldRequired;
        }
    }

    public static class SerializableAction<T> {
        public final AutomationActionType type;
        public final T value;
        @JsonIgnore
        public AutomationAction action;

        @JsonCreator
        public SerializableAction(@JsonProperty("type") AutomationActionType type,
                                  @JsonProperty("value") T value) {
            this.type = type;
            this.value = value;
        }

        @JsonIgnore
        public AutomationAction getAction() {
            return action;
        }

        public synchronized void setAction(AutomationAction action) {
            this.action = action;
        }
    }

    public static class Threshold {
        public final ThresholdAggregation aggregation;
        public final String fieldName;
        public final long value;

        @JsonCreator
        public Threshold(@JsonProperty("aggregation") ThresholdAggregation aggregation,
                         @JsonProperty("fieldName") String fieldName,
                         @JsonProperty("value") long value) {
            this.aggregation = aggregation;
            this.fieldName = fieldName;
            this.value = value;
        }
    }

    public static class ScenarioStep {
        private static final Threshold DEFAULT_THRESHOLD = new Threshold(ThresholdAggregation.count, null, 0);
        public final String collection;
        public final Threshold threshold;
        @JsonIgnore
        public final Predicate<Event> filterPredicate;
        @JsonProperty
        private final String filter;

        @JsonCreator
        public ScenarioStep(@JsonProperty("collection") String collection,
                            @JsonProperty("filter") String filterExpression,
                            @JsonProperty("threshold") Threshold threshold) {
            this.collection = collection;
            this.filter = filterExpression;
            this.threshold = threshold == null ? DEFAULT_THRESHOLD : threshold;

            if (filterExpression == null || filterExpression.isEmpty()) {
                filterPredicate = (event) -> true;
            } else {
                try {
                    filterPredicate = ExpressionCompiler.compile(filterExpression);
                } catch (UnsupportedOperationException e) {
                    throw new RakamException("Unable to compile filter expression", HttpResponseStatus.NOT_IMPLEMENTED);
                }
            }
        }
    }
}

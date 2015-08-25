/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.realtime.AggregationType;
import org.rakam.report.QueryResult;

import java.time.LocalDate;
import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/08/15 07:17.
 */
public interface EventExplorer {
    CompletableFuture<QueryResult> analyze(String project, Measure measureType, String grouping, String segment, String filterExpression, LocalDate startDate, LocalDate endDate);

    public enum TimestampTransformation {
        HOUR, DAY, WEEK, MONTH, QUARTER, DAY_PART, DAY_OF_WEEK;

        @JsonCreator
        public static TimestampTransformation fromString(String key) {
            return key == null ? null : valueOf(key.toUpperCase());
        }
    }

    public static class Measure {
        private final String column;
        private final AggregationType aggregation;

        public Measure(String column, AggregationType aggregation) {
            this.column = column;
            this.aggregation = aggregation;
        }

        public String getColumn() {
            return column;
        }

        public AggregationType getAggregation() {
            return aggregation;
        }
    }
}

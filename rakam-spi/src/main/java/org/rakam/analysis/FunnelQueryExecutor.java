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

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.report.QueryExecution;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static org.rakam.util.ValidationUtil.checkCollection;

public interface FunnelQueryExecutor {

    QueryExecution query(String project, List<FunnelStep> steps, Optional<String> dimension, LocalDate startDate, LocalDate endDate, boolean groupOthers);

    class FunnelStep {
        private static SqlParser parser = new SqlParser();
        public final String collection;
        public final String filterExpression;

        @JsonCreator
        public FunnelStep(@JsonProperty("collection") String collection,
                          @JsonProperty("filterExpression") String filterExpression) {
            checkCollection(collection);
            this.collection = collection;
            this.filterExpression = filterExpression;
        }

        @JsonIgnore
        public synchronized Expression getExpression() {
            return parser.createExpression(filterExpression);
        }
    }
}

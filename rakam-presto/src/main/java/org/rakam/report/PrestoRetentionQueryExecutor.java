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
package org.rakam.report;

import com.google.inject.Inject;
import org.rakam.analysis.RetentionQueryExecutor;

import java.time.LocalDate;
import java.util.Optional;

/**
 * Created by buremba <Burak Emre Kabakcı> on 27/08/15 06:49.
 */
public class PrestoRetentionQueryExecutor implements RetentionQueryExecutor {

    private final PrestoQueryExecutor executor;
    private final String coldStorageConnector;

    @Inject
    public PrestoRetentionQueryExecutor(PrestoQueryExecutor executor, PrestoConfig config) {
        this.executor = executor;
        coldStorageConnector = config.getColdStorageConnector();
    }

    @Override
    public QueryExecution query(String project, Optional<RetentionAction> firstAction, Optional<RetentionAction> returningAction, Optional<String> dimension, LocalDate startDate, LocalDate endDate) {
        return null;
    }
}

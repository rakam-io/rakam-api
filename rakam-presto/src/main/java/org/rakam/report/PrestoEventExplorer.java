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

import com.google.common.collect.ImmutableMap;
import org.rakam.collection.event.metastore.Metastore;

import javax.inject.Inject;
import java.util.Map;

import static org.rakam.analysis.EventExplorer.TimestampTransformation.*;

public class PrestoEventExplorer extends AbstractEventExplorer {
    private static final Map<TimestampTransformation, String> timestampMapping = ImmutableMap.
            <TimestampTransformation, String>builder()
            .put(HOUR_OF_DAY, "hour(%s)")
            .put(DAY_OF_MONTH, "day(%s)")
            .put(WEEK_OF_YEAR, "week(%s)")
            .put(MONTH_OF_YEAR, "month(%s)")
            .put(QUARTER_OF_YEAR, "quarter(%s)")
            .put(DAY_OF_WEEK, "day_of_week(%s)")
            .put(HOUR, "date_trunc('hour', %s)")
            .put(DAY, "cast(%s as date)")
            .put(WEEK, "date_trunc('week', %s)")
            .put(MONTH, "date_trunc('month', %s)")
            .put(YEAR, "date_trunc('year', %s)")
            .build();

    @Inject
    public PrestoEventExplorer(QueryExecutorService service, PrestoQueryExecutor executor, Metastore metastore) {
        super(executor, service, metastore, timestampMapping);
    }
}
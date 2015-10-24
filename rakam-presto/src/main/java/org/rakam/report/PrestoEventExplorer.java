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
import static org.rakam.report.PrestoContinuousQueryService.PRESTO_STREAMING_CATALOG_NAME;

public class PrestoEventExplorer extends AbstractEventExplorer {
    private static final Map<TimestampTransformation, String> timestampMapping = ImmutableMap.
            <TimestampTransformation, String>builder()
            .put(HOUR_OF_DAY, "hour(%s) as time")
            .put(DAY_OF_MONTH, "day(%s) as time")
            .put(WEEK_OF_YEAR, "week(%s) as time")
            .put(MONTH_OF_YEAR, "month(%s) as time")
            .put(QUARTER_OF_YEAR, "quarter(%s) as time")
            .put(DAY_OF_WEEK, "day_of_week(%s) as time")
            .put(HOUR, "date_trunc('hour', %s) as time")
            .put(DAY, "cast(%s as date) as time")
            .put(MONTH, "date_trunc('month', %s) as time")
            .put(YEAR, "date_trunc('year', %s) as time")
            .build();

    @Inject
    public PrestoEventExplorer(PrestoQueryExecutor executor, Metastore metastore) {
        super(executor, metastore, timestampMapping, "from_unixtime");
    }


    @Override
    public String getContinuousTableName(String project, String collection) {
        return String.format("%s.%.%s",
                PRESTO_STREAMING_CATALOG_NAME, project, collection);
    }
}
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
package org.rakam.analysis.postgresql;

import org.rakam.collection.event.metastore.Metastore;
import org.rakam.report.AbstractRetentionQueryExecutor;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;

import javax.inject.Inject;

public class PostgresqlRetentionQueryExecutor extends AbstractRetentionQueryExecutor {

    @Inject
    public PostgresqlRetentionQueryExecutor(PostgresqlQueryExecutor executor, Metastore metastore) {
        super(executor, metastore);
    }


    @Override
    public String convertTimestampFunction() {
        return "to_timestamp";
    }

    @Override
    public String diffTimestamps() {
        return "date_part('%s', age(%s, %s))";
    }

    @Override
    public String getTableReference(String project, String collection) {
        return project +"."+collection;
    }
}


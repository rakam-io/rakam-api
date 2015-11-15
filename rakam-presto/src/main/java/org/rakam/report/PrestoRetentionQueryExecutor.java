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

import org.rakam.collection.event.metastore.Metastore;

import javax.inject.Inject;

public class PrestoRetentionQueryExecutor extends AbstractRetentionQueryExecutor {
    private final PrestoConfig config;

    @Inject
    public PrestoRetentionQueryExecutor(PrestoQueryExecutor executor, Metastore metastore, PrestoConfig config) {
        super(executor, metastore);
        this.config = config;
    }

    @Override
    public String convertTimestampFunction() {
        return "from_unixtime";
    }

    @Override
    public String diffTimestamps() {
        return "date_diff(%s, %s, %s)";
    }

    @Override
    public String getTableReference(String project, String collection) {
        return config.getColdStorageConnector() + "." + project +"." + collection;
    }
}


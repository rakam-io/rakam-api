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
package org.rakam.aws.kinesis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.plugin.stream.CollectionStreamQuery;
import org.rakam.server.http.annotations.ApiParam;

import java.util.List;

public class StreamQuery {
    public final String project;
    public final List<CollectionStreamQuery> collections;

    @JsonCreator
    public StreamQuery(@ApiParam("project") String project,
                       @ApiParam("collections") List<CollectionStreamQuery> collections) {
        this.project = project;
        this.collections = collections;
    }

    @JsonProperty
    public String getProject() {
        return project;
    }

    @JsonProperty
    public List<CollectionStreamQuery> getCollections() {
        return collections;
    }
}

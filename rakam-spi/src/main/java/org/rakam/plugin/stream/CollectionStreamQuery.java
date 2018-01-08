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
package org.rakam.plugin.stream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class CollectionStreamQuery {
    private final String collection;
    private final String filter;

    @JsonCreator
    public CollectionStreamQuery(@JsonProperty("collection") String collection,
                                 @JsonProperty("filter") String filter) {
        this.collection = collection;
        this.filter = filter;
    }

    @JsonProperty
    public String getCollection() {
        return collection;
    }

    @JsonProperty
    public String getFilter() {
        return filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CollectionStreamQuery that = (CollectionStreamQuery) o;

        if (!collection.equals(that.collection)) {
            return false;
        }
        return filter != null ? filter.equals(that.filter) : that.filter == null;
    }

    @Override
    public int hashCode() {
        int result = collection.hashCode();
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        return result;
    }
}
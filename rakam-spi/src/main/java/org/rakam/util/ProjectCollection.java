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
package org.rakam.util;

import static org.rakam.util.ValidationUtil.checkNotNull;

public class ProjectCollection {
    public final String project;
    public final String collection;

    public ProjectCollection(String project, String collection) {
        this.project = checkNotNull(project, "project is null");
        this.collection = checkNotNull(collection, "collection is null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProjectCollection)) return false;

        ProjectCollection that = (ProjectCollection) o;

        if (!project.equals(that.project)) return false;
        return collection.equals(that.collection);

    }

    @Override
    public int hashCode() {
        int result = project.hashCode();
        result = 31 * result + collection.hashCode();
        return result;
    }
}

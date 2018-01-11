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
package org.rakam.ui.page;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public interface CustomPageDatabase {
    void save(Integer user, int project, Page page);

    List<Page> list(int project);

    Map<String, String> get(int project, String slug);

    InputStream getFile(int project, String name, String file);

    void delete(int project, String slug);

    class Page {
        public final String name;
        public final String slug;
        public final String category;
        public final Map<String, String> files;

        @JsonCreator
        public Page(@JsonProperty(value = "name", required = true) String name,
                    @JsonProperty(value = "slug", required = true) String slug,
                    @JsonProperty(value = "category", required = true) String category,
                    @JsonProperty(value = "files", required = true) Map<String, String> files) {
            this.name = name;
            this.slug = slug;
            this.category = category;
            this.files = checkNotNull(files);
        }

        public Page(String name, String slug, String category) {
            this.name = name;
            this.slug = slug;
            this.category = category;
            this.files = null;
        }
    }
}

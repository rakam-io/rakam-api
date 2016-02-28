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
import org.rakam.plugin.ProjectItem;
import org.rakam.server.http.annotations.ApiParam;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;


public interface CustomPageDatabase {
    void save(Integer user, Page page);

    List<Page> list(String project);

    Map<String, String> get(String project, String slug);

    InputStream getFile(String project, String name, String file);

    void delete(String project, String slug);

    class Page implements ProjectItem {
        public final String project;
        public final String name;
        public final String slug;
        public final String category;
        public final Map<String, String> files;

        @JsonCreator
        public Page(@ApiParam(name = "project") String project,
                    @ApiParam(name = "name") String name,
                    @ApiParam(name = "slug") String slug,
                    @ApiParam(name = "category") String category,
                    @ApiParam(name = "files") Map<String, String> files) {
            this.project = project;
            this.name = name;
            this.slug = slug;
            this.category = category;
            this.files = checkNotNull(files);
        }

        public Page(String project, String name, String slug, String category) {
            this.project = project;
            this.name = name;
            this.slug = slug;
            this.category = category;
            this.files = null;
        }

        @Override
        public String project() {
            return project;
        }
    }
}

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

import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Charsets.UTF_8;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;


public class JDBCCustomPageDatabase implements CustomPageDatabase {

    private final DBI dbi;

    @Inject
    public JDBCCustomPageDatabase(@Named("ui.metadata.jdbc") JDBCPoolDataSource dataSource) {
        dbi = new DBI(dataSource);
    }

    public void save(Integer user, int project, Page page) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO custom_page (project_id, name, slug, category, data, user_id) VALUES (:project, :name, :slug, :category, :data, :user)")
                    .bind("project", project)
                    .bind("name", page.name)
                    .bind("slug", page.slug)
                    .bind("category", page.category)
                    .bind("user", user)
                    .bind("data", JsonHelper.encode(page.files)).execute();
        } catch (Exception e) {
            // TODO move it to transaction
            if (get(project, page.slug) != null) {
                throw new AlreadyExistsException(String.format("Custom page %s", page.slug), BAD_REQUEST);
            }
            throw e;
        }
    }

    public List<Page> list(int project) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT name, slug, category FROM custom_page WHERE project_id = :project")
                    .bind("project", project)
                    .map((i, resultSet, statementContext) -> {
                        return new Page(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3));
                    }).list();
        }
    }

    public Map<String, String> get(int project, String slug) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT data FROM custom_page WHERE project_id = :project AND slug = :slug")
                    .bind("project", project)
                    .bind("slug", slug)
                    .map((i, resultSet, statementContext) -> {
                        return JsonHelper.read(resultSet.getString(1), Map.class);
                    }).first();
        }
    }

    @Override
    public InputStream getFile(int project, String slug, String file) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT data FROM custom_page WHERE project_id = :project AND slug = :slug")
                    .bind("project", project)
                    .bind("slug", slug)
                    .map((i, resultSet, statementContext) -> {
                        return new ByteArrayInputStream(((String) JsonHelper.read(resultSet.getString(1), Map.class)
                                .get(file)).getBytes(UTF_8));
                    }).first();
        }
    }

    @Override
    public void delete(int project, String slug) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM custom_page WHERE project_id = :project AND slug = :slug")
                    .bind("project", project)
                    .bind("slug", slug).execute();
        }
    }
}

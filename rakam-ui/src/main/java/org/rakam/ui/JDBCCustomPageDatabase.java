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
package org.rakam.ui;

import com.google.inject.name.Named;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;


public class JDBCCustomPageDatabase implements CustomPageDatabase {

    private final DBI dbi;

    @Inject
    public JDBCCustomPageDatabase(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        dbi = new DBI(dataSource);
        setup();
    }

    public void setup() {
        dbi.inTransaction((handle, transactionStatus) ->
                handle.createStatement("CREATE TABLE IF NOT EXISTS custom_page (" +
                        "  project VARCHAR(255) NOT NULL," +
                        "  name VARCHAR(255) NOT NULL," +
                        "  slug VARCHAR(255) NOT NULL," +
                        "  category VARCHAR(255)," +
                        "  data TEXT NOT NULL," +
                        "  PRIMARY KEY (project, name)" +
                        "  )")
                        .execute());
    }

    public void save(String project, String name, String slug, String category, Map<String, String> files) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO custom_page (project, name, slug, category, data) VALUES (:project, :name, :slug, :category, :data)")
                    .bind("project", project)
                    .bind("name", name)
                    .bind("slug", slug)
                    .bind("category", category)
                    .bind("data", JsonHelper.encode(files)).execute();
        } catch (Exception e) {
            // TODO move it to transaction
            if (get(project, name) != null) {
                throw new RakamException("Report already exists", HttpResponseStatus.BAD_REQUEST);
            }
            throw e;
        }
    }

    public List<Page> list(String project) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT name, slug, category FROM custom_page WHERE project = :project")
                    .bind("project", project)
                    .map((i, resultSet, statementContext) -> {
                        return new Page(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3));
                    }).list();
        }
    }

    public Map<String, String> get(String project, String slug) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT data FROM custom_page WHERE project = :project AND slug = :slug")
                    .bind("project", project)
                    .bind("slug", slug)
                    .map((i, resultSet, statementContext) -> {
                        return JsonHelper.read(resultSet.getString(1), Map.class);
                    }).first();
        }
    }

    @Override
    public InputStream getFile(String project, String slug, String file) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT data FROM custom_page WHERE project = :project AND slug = :slug")
                    .bind("project", project)
                    .bind("slug", slug)
                    .map((i, resultSet, statementContext) -> {
                        return new ByteArrayInputStream(((String) JsonHelper.read(resultSet.getString(1), Map.class)
                                .get(file)).getBytes(Charset.forName("UTF-8")));
                    }).first();
        }
    }

    @Override
    public void delete(String project, String name) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM custom_page WHERE project = :project AND slug = :slug)")
                    .bind("project", project)
                    .bind("slug", name).execute();
        }
    }
}

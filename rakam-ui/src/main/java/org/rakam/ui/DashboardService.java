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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.rakam.plugin.JDBCConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.StringMapper;

import javax.ws.rs.Path;
import java.util.List;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 12/09/15 18:58.
 */
@Path("/ui/dashboard")
@Api(value = "/ui/dashboard", description = "Dashboard service", tags = "rakam-ui")
public class DashboardService extends HttpService {
    private final Handle dao;

    @Inject
    public DashboardService(@Named("ui.metadata.store.jdbc") JDBCConfig config) {
        DBI dbi = new DBI(format(config.getUrl(), config.getUsername(), config.getPassword()),
                config.getUsername(), config.getPassword());
        dao = dbi.open();
        setup();
    }

    public void setup() {
        dao.createStatement("CREATE TABLE IF NOT EXISTS dashboard (" +
                "  id SERIAL," +
                "  project VARCHAR(255) NOT NULL," +
                "  name VARCHAR(255) NOT NULL," +
                "  PRIMARY KEY (id)" +
                "  )")
                .execute();
        dao.createStatement("CREATE TABLE IF NOT EXISTS dashboard_items (" +
                "  id SERIAL," +
                "  dashboard int NOT NULL REFERENCES dashboard(id) ON DELETE CASCADE," +
                "  name VARCHAR(255) NOT NULL," +
                "  data TEXT NOT NULL," +
                "  PRIMARY KEY (id)" +
                "  )")
                .execute();
    }

    @JsonRequest
    @Path("/create")
    public void create(@ApiParam("project") String project,
                       @ApiParam("name") String name) {
        try {
            dao.createStatement("INSERT INTO dashboard (project, name) VALUES (:project, :name)")
                    .bind("project", project)
                    .bind("name", name).execute();
        } catch (Exception e) {
            // TODO move it to transaction
            if (get(project, name) != null) {
                throw new RakamException("Dashboard already exists", 400);
            }
            throw e;
        }
    }

    @JsonRequest
    @Path("/delete")
    public void delete(@ApiParam(name="project") String project,
                       @ApiParam(name="name") String name) {
        dao.createStatement("DELETE FROM dashboard WHERE project = :project AND name = :name")
                    .bind("project", project)
                    .bind("name", name).execute();
    }

    @JsonRequest
    @Path("/get")
    public List<DashboardItem> get(@ApiParam(name="project") String project,
                                   @ApiParam(name="name") String name) {
        return dao.createQuery("SELECT id, data FROM dashboard_items WHERE dashboard = (SELECT id FROM dashboard WHERE name = :name)")
                .bind("name", name).map((i, r, statementContext) -> {
                    return new DashboardItem(r.getInt(1), JsonHelper.read(r.getString(2), ObjectNode.class));
                }).list();
    }

    @JsonRequest
    @Path("/list")
    public List<String> list(@ApiParam(name="project") String project) {
        return dao.createQuery("SELECT name FROM dashboard WHERE project = :project")
                .bind("project", project).map(StringMapper.FIRST).list();
    }

    public static class DashboardItem {
        public final int id;
        public final ObjectNode data;

        public DashboardItem(int id, ObjectNode data) {
            this.id = id;
            this.data = data;
        }
    }

    @JsonRequest
    @Path("/add_item")
    public void addToDashboard(@ApiParam(name="project") String project,
                               @ApiParam(name="dashboardName") String dashboardName,
                               @ApiParam(name="name") String itemName,
                               @ApiParam(name="data") ObjectNode data) {
        dao.createStatement("INSERT INTO dashboard_items (dashboard, name, data) VALUES ((SELECT id FROM dashboard WHERE project = :project AND name = :dashboardName), :name, :data)")
                .bind("project", project)
                .bind("dashboardName", dashboardName)
                .bind("name", itemName)
                .bind("data", JsonHelper.encode(data)).execute();
    }

    @JsonRequest
    @Path("/remove_item")
    public void removeFromDashboard(@ApiParam(name="project") String project,
                                    @ApiParam(name="dashboardName") String dashboardName,
                                    @ApiParam(name="name") String itemName) {
        dao.createStatement("DELETE FROM dashboard_items WHERE dashboard = (SELECT id FROM dashboard WHERE project = :project AND name = :dashboardName) AND name = :name")
                .bind("project", project)
                .bind("dashboardName", dashboardName)
                .bind("name", itemName).execute();
    }
}

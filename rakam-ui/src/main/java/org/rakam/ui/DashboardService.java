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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import javax.inject.Inject;
import com.google.inject.name.Named;
import org.rakam.plugin.JDBCConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;
import org.rakam.util.JsonResponse;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.IntegerMapper;

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
                "  directive VARCHAR(255) NOT NULL," +
                "  data TEXT NOT NULL," +
                "  PRIMARY KEY (id)" +
                "  )")
                .execute();
    }

    @JsonRequest
    @Path("/create")
    public Dashboard create(@ApiParam(name="project") String project,
                            @ApiParam(name="name") String name) {
        int id = dao.createQuery("INSERT INTO dashboard (project, name) VALUES (:project, :name) RETURNING id")
                    .bind("project", project)
                    .bind("name", name).map(IntegerMapper.FIRST).first();
        return new Dashboard(id, name);
    }

    @JsonRequest
    @Path("/delete")
    public JsonResponse delete(@ApiParam(name = "project") String project,
                               @ApiParam(name = "name") String name) {
        dao.createStatement("DELETE FROM dashboard WHERE project = :project AND name = :name")
                    .bind("project", project)
                    .bind("name", name).execute();
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/get")
    public List<DashboardItem> get(@ApiParam(name="project") String project,
                                   @ApiParam(name="id") int id) {
        return dao.createQuery("SELECT id, name, directive, data FROM dashboard_items WHERE dashboard = :id")
                .bind("id", id).map((i, r, statementContext) -> {
                    return new DashboardItem(r.getInt(1), r.getString(2), r.getString(3), JsonHelper.read(r.getString(4), JsonNode.class));
                }).list();
    }

    @JsonRequest
    @Path("/list")
    public List<Dashboard> list(@ApiParam(name="project") String project) {
        return dao.createQuery("SELECT id, name FROM dashboard WHERE project = :project")
                .bind("project", project).map((i, resultSet, statementContext) -> {
                    return new Dashboard(resultSet.getInt(1), resultSet.getString(2));
                }).list();
    }

    public static class Dashboard {
        public final int id;
        public final String name;

        public Dashboard(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public static class DashboardItem {
        public final int id;
        public final JsonNode data;
        public final String directive;
        public final String name;

        @JsonCreator
        public DashboardItem(@JsonProperty("id") int id,
                             @JsonProperty("name") String name,
                             @JsonProperty("directive") String directive,
                             @JsonProperty("data") JsonNode data) {
            this.id = id;
            this.directive = directive;
            this.name = name;
            this.data = data;
        }
    }

    @JsonRequest
    @Path("/add_item")
    public JsonResponse addToDashboard(@ApiParam(name="project") String project,
                               @ApiParam(name="dashboard") int dashboard,
                               @ApiParam(name="name") String itemName,
                               @ApiParam(name="directive") String directive,
                               @ApiParam(name="data") JsonNode data) {
        dao.createStatement("INSERT INTO dashboard_items (dashboard, name, directive, data) VALUES (:dashboard, :name, :directive, :data)")
                .bind("project", project)
                .bind("dashboard", dashboard)
                .bind("name", itemName)
                .bind("directive", directive)
                .bind("data", JsonHelper.encode(data)).execute();
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/update_dashboard")
    public JsonResponse updateDashboard(@ApiParam(name="project") String project,
                               @ApiParam(name="dashboard") int dashboard,
                               @ApiParam(name="items") List<DashboardItem> items) {

        dao.inTransaction((handle, transactionStatus) -> {
            for (DashboardItem item : items) {
                handle.createStatement("UPDATE dashboard_items SET name = :name, directive = :directive, data = :data WHERE id = :id")
                        .bind("id", item.id)
                        .bind("name", item.name)
                        .bind("directive", item.directive)
                        .bind("data", JsonHelper.encode(item.data)).execute();
            }
            return null;
        });
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/rename_item")
    public JsonResponse renameDashboardItem(@ApiParam(name="project") String project,
                               @ApiParam(name="dashboard") int dashboard,
                               @ApiParam(name="id") int id,
                               @ApiParam(name="name") int name) {

        dao.createStatement("UPDATE dashboard_items SET name = :name WHERE id = :id")
                .bind("id", id)
                .bind("name", name).execute();
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/remove_item")
    public JsonResponse removeFromDashboard(@ApiParam(name = "project") String project,
                                            @ApiParam(name = "dashboard") int dashboard,
                                            @ApiParam(name = "id") int id) {
        dao.createStatement("DELETE FROM dashboard_items WHERE dashboard = :dashboard AND id = :id")
                .bind("project", project)
                .bind("dashboard", dashboard)
                .bind("id", id).execute();
        return JsonResponse.success();
    }
}

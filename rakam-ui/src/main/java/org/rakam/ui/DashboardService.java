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
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;
import org.rakam.util.JsonResponse;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.IntegerMapper;

import javax.inject.Inject;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Map;


@Path("/ui/dashboard")
@IgnoreApi
public class DashboardService extends HttpService {
    private final DBI dbi;

    @Inject
    public DashboardService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        dbi = new DBI(dataSource);
        setup();
    }

    public void setup() {
        dbi.inTransaction((handle, transactionStatus) -> {
            handle.createStatement("CREATE TABLE IF NOT EXISTS dashboard (" +
                    "  id SERIAL," +
                    "  project VARCHAR(255) NOT NULL," +
                    "  user_id INT NOT NULL REFERENCES web_user(id)," +
                    "  name VARCHAR(255) NOT NULL," +
                    "  options TEXT," +
                    "  UNIQUE (project, name)," +
                    "  PRIMARY KEY (id)" +
                    "  )")
                    .execute();
            handle.createStatement("CREATE TABLE IF NOT EXISTS dashboard_items (" +
                    "  id SERIAL," +
                    "  dashboard int NOT NULL REFERENCES dashboard(id) ON DELETE CASCADE," +
                    "  name VARCHAR(255) NOT NULL," +
                    "  directive VARCHAR(255) NOT NULL," +
                    "  data TEXT NOT NULL," +
                    "  PRIMARY KEY (id)" +
                    "  )")
                    .execute();
            return null;
        });
    }

    @JsonRequest
    @ApiOperation(value = "Create dashboard", authorizations = @Authorization(value = "read_key"))
    @Path("/create")
    public Dashboard create(@ApiParam(name="project") String project,
                            @ApiParam(name="name") String name,
                            @ApiParam(name="options", required = false) Map<String, Object> options) {
        try(Handle handle = dbi.open()) {
            int id = handle.createQuery("INSERT INTO dashboard (project, name, options) VALUES (:project, :name, :options) RETURNING id")
                    .bind("project", project)
                    .bind("options", JsonHelper.encode(options))
                    .bind("name", name).map(IntegerMapper.FIRST).first();
            return new Dashboard(id, name, options);
        }
    }

    @JsonRequest
    @ApiOperation(value = "Create dashboard", authorizations = @Authorization(value = "read_key"))
    @Path("/delete")
    public JsonResponse delete(@ApiParam(name = "project", required = true) String project,
                               @ApiParam(name = "name", required = true) String name) {
        try(Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM dashboard WHERE project = :project AND name = :name")
                    .bind("project", project)
                    .bind("name", name).execute();
        }
        return JsonResponse.success();
    }

    @JsonRequest
    @ApiOperation(value = "Get Report", authorizations = @Authorization(value = "read_key"))
    @Path("/get")
    public List<DashboardItem> get(@ApiParam(name="project") String project,
                                   @ApiParam(name="name") String name) {
        try(Handle handle = dbi.open()) {
            return handle.createQuery("SELECT id, name, directive, data FROM dashboard_items WHERE dashboard = (SELECT id FROM dashboard WHERE project = :project AND name = :name)")
                    .bind("project", project)
                    .bind("name", name)
                    .map((i, r, statementContext) -> {
                        return new DashboardItem(r.getInt(1), r.getString(2), r.getString(3), JsonHelper.read(r.getString(4), JsonNode.class));
                    }).list();
        }
    }

    @JsonRequest
    @ApiOperation(value = "List Report", authorizations = @Authorization(value = "read_key"))
    @Path("/list")
    public List<Dashboard> list(@ApiParam(name="project", required = true) String project) {
        try(Handle handle = dbi.open()) {
            return handle.createQuery("SELECT id, name, options FROM dashboard WHERE project = :project ORDER BY id")
                    .bind("project", project).map((i, resultSet, statementContext) -> {
                        String options = resultSet.getString(3);
                        return new Dashboard(resultSet.getInt(1), resultSet.getString(2),
                                options == null ? null : JsonHelper.read(options, Map.class));
                    }).list();
        }
    }

    public static class Dashboard {
        public final int id;
        public final String name;
        public final Map<String, Object> options;

        public Dashboard(int id, String name, Map<String, Object> options) {
            this.id = id;
            this.name = name;
            this.options = options;
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
    @ApiOperation(value = "Add item to dashboard", authorizations = @Authorization(value = "read_key"))
    @Path("/add_item")
    public JsonResponse addToDashboard(@ApiParam(name="project") String project,
                               @ApiParam(name="dashboard") int dashboard,
                               @ApiParam(name="name") String itemName,
                               @ApiParam(name="directive") String directive,
                               @ApiParam(name="data") JsonNode data) {
        try(Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO dashboard_items (dashboard, name, directive, data) VALUES (:dashboard, :name, :directive, :data)")
                    .bind("project", project)
                    .bind("dashboard", dashboard)
                    .bind("name", itemName)
                    .bind("directive", directive)
                    .bind("data", JsonHelper.encode(data)).execute();
        }
        return JsonResponse.success();
    }

    @JsonRequest
    @ApiOperation(value = "Update dashboard items", authorizations = @Authorization(value = "read_key"))
    @Path("/update_dashboard_items")
    public JsonResponse updateDashboard(@ApiParam(name="project") String project,
                               @ApiParam(name="dashboard") int dashboard,
                               @ApiParam(name="items") List<DashboardItem> items) {

        dbi.inTransaction((handle, transactionStatus) -> {
            for (DashboardItem item : items) {

                // TODO: verify dashboard is in project
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
    @ApiOperation(value = "Update dashboard options", authorizations = @Authorization(value = "read_key"))
    @Path("/update_dashboard_options")
    public JsonResponse updateDashboardOptions(@ApiParam(name="project") String project,
                               @ApiParam(name="dashboard") int dashboard,
                               @ApiParam(name="options") Map<String, Object> options) {

        dbi.inTransaction((handle, transactionStatus) -> {
            handle.createStatement("UPDATE dashboard SET options = :options WHERE id = :id AND project = :project")
                    .bind("id", dashboard)
                    .bind("options", JsonHelper.encode(options))
                    .bind("project", project)
                    .execute();
            return null;
        });
        return JsonResponse.success();
    }

    @JsonRequest
    @ApiOperation(value = "Rename dashboard item", authorizations = @Authorization(value = "read_key"))
    @Path("/rename_item")
    public JsonResponse renameDashboardItem(@ApiParam(name="project") String project,
                               @ApiParam(name="dashboard") int dashboard,
                               @ApiParam(name="id") int id,
                               @ApiParam(name="name") String name) {
        try(Handle handle = dbi.open()) {
            handle.createStatement("UPDATE dashboard_items SET name = :name WHERE id = :id")
                    .bind("id", id)
                    .bind("name", name).execute();
        }
        return JsonResponse.success();
    }

    @JsonRequest
    @ApiOperation(value = "Delete dashboard item", authorizations = @Authorization(value = "read_key"))
    @Path("/delete_item")
    public JsonResponse removeFromDashboard(@ApiParam(name = "project") String project,
                                            @ApiParam(name = "dashboard") int dashboard,
                                            @ApiParam(name = "id") int id) {
        try(Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM dashboard_items WHERE dashboard = :dashboard AND id = :id")
                    .bind("project", project)
                    .bind("dashboard", dashboard)
                    .bind("id", id).execute();
        }
        return JsonResponse.success();
    }
}

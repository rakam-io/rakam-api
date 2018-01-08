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
package org.rakam.ui.customreport;

import com.google.inject.name.Named;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.util.StringMapper;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class JDBCCustomReportMetadata implements CustomReportMetadata {
    private final DBI dbi;

    @Inject
    public JDBCCustomReportMetadata(@Named("ui.metadata.jdbc") JDBCPoolDataSource dataSource) {
        dbi = new DBI(dataSource);
        createIndexIfNotExists();
    }

    private void createIndexIfNotExists() {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE INDEX report_type_idx ON custom_reports(report_type, project)")
                    .execute();
        } catch (UnableToExecuteStatementException e) {
            // IF NOT EXIST feature is not supported by majority of RDBMSs.
            // Since this INDEX is optional, swallow exception
            // since the exception is probably about duplicate indexes.
        }
    }

    @Override
    public void save(Integer user, int project, CustomReport report) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO custom_reports (report_type, project_id, name, data, user_id) VALUES (:reportType, :project, :name, :data, :user)")
                    .bind("reportType", report.reportType)
                    .bind("project", project)
                    .bind("name", report.name)
                    .bind("user", user)
                    .bind("data", JsonHelper.encode(report.data)).execute();
        } catch (Exception e) {
            // TODO move it to transaction
            if (get(report.reportType, project, report.name) != null) {
                throw new AlreadyExistsException("Custom report", HttpResponseStatus.BAD_REQUEST);
            }
            throw e;
        }
    }

    @Override
    public CustomReport get(String reportType, int project, String name) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT data, web_user.id, web_user.email FROM custom_reports r " +
                    "JOIN web_user ON (web_user.id = user_id) " +
                    "WHERE r.report_type = :reportType AND r.project_id = :project AND r.name = :name")
                    .bind("reportType", reportType)
                    .bind("project", project)
                    .bind("name", name)
                    .map((i, resultSet, statementContext) -> {
                        return new CustomReport(reportType, name, resultSet.getInt(2),
                                resultSet.getString(3), JsonHelper.read(resultSet.getString(1)));
                    }).first();
        }
    }

    @Override
    public List<CustomReport> list(String reportType, int project) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT r.name, r.data, r.user_id, email FROM custom_reports r " +
                    "JOIN web_user ON (web_user.id = user_id) WHERE report_type = :reportType AND project_id = :project")
                    .bind("reportType", reportType)
                    .bind("project", project)
                    .map((i, resultSet, statementContext) -> {
                        return new CustomReport(reportType, resultSet.getString(1), resultSet.getInt(3), resultSet.getString(4), JsonHelper.read(resultSet.getString(2)));
                    }).list();
        }
    }

    @Override
    public Map<String, List<CustomReport>> list(int project) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT r.report_type, r.name, r.data, r.user_id, email FROM custom_reports r     " +
                    "JOIN web_user ON (web_user.id = user_id) WHERE project_id = :project")
                    .bind("project", project)
                    .map((i, resultSet, statementContext) -> {
                        return new CustomReport(resultSet.getString(1), resultSet.getString(2), resultSet.getInt(4), resultSet.getString(5), JsonHelper.read(resultSet.getString(3)));
                    }).list().stream().collect(Collectors.groupingBy(customReport -> customReport.reportType));
        }
    }

    @Override
    public void delete(String reportType, int project, String name) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM custom_reports WHERE report_type = :reportType AND project_id = :project AND name = :name")
                    .bind("reportType", reportType)
                    .bind("project", project)
                    .bind("name", name).execute();
        }
    }

    @Override
    public void update(int project, CustomReport report) {
        int execute;
        try (Handle handle = dbi.open()) {
            execute = handle.createStatement("UPDATE custom_reports SET data = :data WHERE report_type = :reportType AND name = :name AND project_id = :project")
                    .bind("reportType", report.reportType)
                    .bind("project", project)
                    .bind("name", report.name)
                    .bind("data", JsonHelper.encode(report.data)).execute();
        }
        if (execute == 0) {
            throw new RakamException("Report does not exist.", HttpResponseStatus.BAD_REQUEST);
        }
    }

    @Override
    public List<String> types(int project) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT DISTINCT report_type FROM custom_reports WHERE project_id = :project")
                    .bind("project", project)
                    .map(StringMapper.FIRST).list();
        }
    }
}

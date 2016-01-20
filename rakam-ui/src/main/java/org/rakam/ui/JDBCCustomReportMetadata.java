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
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.util.StringMapper;

import javax.inject.Inject;
import java.util.List;


public class JDBCCustomReportMetadata {
    private final DBI dbi;

    @Inject
    public JDBCCustomReportMetadata(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        dbi = new DBI(dataSource);
        setup();
        createIndexIfNotExists();
    }

    public void setup() {
        try(Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS custom_reports (" +
                    "  report_type VARCHAR(255) NOT NULL," +
                    "  project VARCHAR(255) NOT NULL," +
                    "  name VARCHAR(255) NOT NULL," +
                    "  data TEXT NOT NULL," +
                    "  PRIMARY KEY (report_type, project, name)" +
                    "  )")
                    .execute();
        }
    }

    private void createIndexIfNotExists() {
        try(Handle handle = dbi.open()) {
            handle.createStatement("CREATE INDEX report_type_idx ON custom_reports(report_type, project)")
                    .execute();
        } catch (UnableToExecuteStatementException e) {
            // IF NOT EXIST feature is not supported by majority of RDBMSs.
            // Since this INDEX is optional, swallow exception
            // since the exception is probably about duplicate indexes.
        }
    }

    public void add(CustomReport report) {
        try(Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO custom_reports (report_type, project, name, data) VALUES (:reportType, :project, :name, :data)")
                    .bind("reportType", report.reportType)
                    .bind("project", report.project)
                    .bind("name", report.name)
                    .bind("data", JsonHelper.encode(report.data)).execute();
        } catch (Exception e) {
            // TODO move it to transaction
            if (get(report.reportType, report.project, report.name) != null) {
                throw new RakamException("Report already exists", HttpResponseStatus.BAD_REQUEST);
            }
            throw e;
        }
    }

    public CustomReport get(String reportType, String project, String name) {
        try(Handle handle = dbi.open()) {
            return handle.createQuery("SELECT data FROM custom_reports WHERE report_type = :reportType AND project = :project AND name = :name")
                    .bind("reportType", reportType)
                    .bind("project", project)
                    .bind("name", name)
                    .map((i, resultSet, statementContext) -> {
                        return new CustomReport(reportType, project, name, JsonHelper.read(resultSet.getString(1)));
                    }).first();
        }
    }

    public List<CustomReport> list(String reportType, String project) {
        try(Handle handle = dbi.open()) {
            return handle.createQuery("SELECT name, data FROM custom_reports WHERE report_type = :reportType AND project = :project")
                    .bind("reportType", reportType)
                    .bind("project", project)
                    .map((i, resultSet, statementContext) -> {
                        return new CustomReport(reportType, project, resultSet.getString(1), JsonHelper.read(resultSet.getString(2)));
                    }).list();
        }
    }

    public void delete(String reportType, String project, String name) {
        try(Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM custom_reports WHERE report_type = :reportType AND project = :project AND name = :name")
                    .bind("reportType", reportType)
                    .bind("project", project)
                    .bind("name", name).execute();
        }
    }

    public void update(CustomReport report) {
        int execute;
        try(Handle handle = dbi.open()) {
            execute = handle.createStatement("UPDATE custom_reports SET data = :data WHERE report_type = :reportType AND name = :name AND project = :project")
                    .bind("reportType", report.reportType)
                    .bind("project", report.project)
                    .bind("name", report.name)
                    .bind("data", JsonHelper.encode(report.data)).execute();
        }
        if(execute == 0) {
            throw new RakamException("Report does not exist.", HttpResponseStatus.BAD_REQUEST);
        }
    }

    public List<String> types(String project) {
        try(Handle handle = dbi.open()) {
            return handle.createQuery("SELECT DISTINCT report_type FROM custom_reports WHERE project = :project")
                    .bind("project", project)
                    .map(StringMapper.FIRST).list();
        }
    }
}

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

import javax.inject.Inject;
import com.google.inject.name.Named;
import org.rakam.plugin.JDBCConfig;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import java.util.List;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 23/08/15 18:45.
 */
public class JDBCCustomReportMetadata {
    private Handle dao;

    @Inject
    public JDBCCustomReportMetadata(@Named("report.metadata.store.jdbc") JDBCConfig config) {

        DBI dbi = new DBI(format(config.getUrl(), config.getUsername(), config.getPassword()),
                config.getUsername(), config.getPassword());
        dao = dbi.open();
        setup();
    }

    public void setup() {
        dao.createStatement("CREATE TABLE IF NOT EXISTS custom_reports (" +
                "  report_type VARCHAR(255) NOT NULL," +
                "  project VARCHAR(255) NOT NULL," +
                "  name VARCHAR(255) NOT NULL," +
                "  data TEXT NOT NULL," +
                "  PRIMARY KEY (report_type, project, name)" +
                "  )")
                .execute();
        try {
            dao.createStatement("CREATE INDEX report_type_idx ON custom_reports(report_type, project)")
                    .execute();
        } catch (UnableToExecuteStatementException e) {
            // IF NOT EXIST feature is not supported by majority of RDBMSs.
            // Since this INDEX is optional, swallow exception
            // since the exception is probably about duplicate indexes.
        }
    }

    public void add(CustomReport report) {
        try {
            dao.createStatement("INSERT INTO custom_reports (report_type, project, name, data) VALUES (:reportType, :project, :name, :data)")
                    .bind("reportType", report.reportType)
                    .bind("project", report.project)
                    .bind("name", report.name)
                    .bind("data", JsonHelper.encode(report.data)).execute();
        } catch (Exception e) {
            // TODO move it to transaction
            if (get(report.reportType, report.project, report.name) != null) {
                throw new RakamException("Report already exists", 400);
            }
            throw e;
        }
    }

    public CustomReport get(String reportType, String project, String name) {
        return dao.createQuery("SELECT data FROM custom_reports WHERE report_type = :reportType AND project = :project AND name = :name")
                .bind("reportType", reportType)
                .bind("project", project)
                .bind("name", name)
                .map((i, resultSet, statementContext) -> {
                    return new CustomReport(reportType, project, name, JsonHelper.read(resultSet.getString(2)));
                }).first();
    }

    public List<CustomReport> list(String reportType, String project) {
        return dao.createQuery("SELECT name, data FROM custom_reports WHERE report_type = :reportType AND project = :project")
                .bind("reportType", reportType)
                .bind("project", project)
                .map((i, resultSet, statementContext) -> {
                    return new CustomReport(reportType, project, resultSet.getString(1), JsonHelper.read(resultSet.getString(2)));
                }).list();
    }

    public void delete(String reportType, String project, String name) {
        dao.createStatement("DELETE FROM custom_reports WHERE report_type = :reportType AND project = :project AND name = :name)")
                .bind("reportType", reportType)
                .bind("project", project)
                .bind("name", name).execute();
    }
}

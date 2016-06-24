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
package org.rakam.postgresql.analysis;

import com.google.common.base.Throwables;
import org.rakam.analysis.AbstractFunnelQueryExecutor;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;

public class PostgresqlFunnelQueryExecutor
        extends AbstractFunnelQueryExecutor
{
    private final PostgresqlQueryExecutor executor;

    @Inject
    public PostgresqlFunnelQueryExecutor(PostgresqlQueryExecutor executor)
    {
        super(executor, '"');
        this.executor = executor;
    }

    @PostConstruct
    public void setup()
    {
        try (Connection conn = executor.getConnection()) {
            conn.createStatement().execute("CREATE OR REPLACE FUNCTION public.get_funnel_step(arr int[]) RETURNS integer AS $$\n" +
                    "DECLARE next_step integer := 1; step integer;\n" +
                    "        BEGIN \n" +
                    "                FOREACH step IN ARRAY arr\n" +
                    "       LOOP\n" +
                    "      IF step = next_step THEN\n" +
                    "         next_step = next_step + 1;\n" +
                    "      END IF;\n" +
                    "       END LOOP;\n" +
                    "    return next_step - 1;\n" +
                    "        END;\n" +
                    "$$ LANGUAGE plpgsql;");
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public String getTemplate()
    {
        return "select %s get_funnel_step(steps) step, count(*) total from (\n" +
                "select %s array_agg(step order by _time) as steps from (%s) t WHERE _time between date '%s' and date '%s'\n" +
                "group by %s %s\n" +
                ") t group by 1 %s order by 1";
    }
}


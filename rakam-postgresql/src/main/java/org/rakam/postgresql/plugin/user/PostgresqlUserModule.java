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
package org.rakam.postgresql.plugin.user;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.config.JDBCConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.user.UserStorage;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.util.ConditionalModule;

@AutoService(RakamModule.class)
@ConditionalModule(config = "plugin.user.storage", value = "postgresql")
public class PostgresqlUserModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        JDBCConfig config = buildConfigObject(JDBCConfig.class, "store.adapter.postgresql");

        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named("store.adapter.postgresql"))
                .toInstance(JDBCPoolDataSource.getOrCreateDataSource(config));

        binder.bind(PostgresqlQueryExecutor.class).in(Scopes.SINGLETON);
        binder.bind(UserStorage.class).to(AbstractPostgresqlUserStorage.class)
                .in(Scopes.SINGLETON);

        binder.bind(boolean.class).annotatedWith(Names.named("user.storage.postgresql"))
                .toInstance("postgresql".equals(true));
    }

    @Override
    public String name() {
        return "Postgresql backend for user storage";
    }

    @Override
    public String description() {
        return "Postgresql user storage backend for basic CRUD and search operations.";
    }
}

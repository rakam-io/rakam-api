package org.rakam.ui;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

public class WebUserService {

    private final DBI dbi;

    @Inject
    public WebUserService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        dbi = new DBI(dataSource);
        setup();
    }

    public void setup() {
        try(Handle handle = dbi.open()) {

            handle.createStatement("CREATE TABLE IF NOT EXISTS web_user (" +
                    "  id SERIAL PRIMARY KEY,\n" +
                    "  email TEXT NOT NULL UNIQUE,\n" +
                    "  password TEXT NOT NULL,\n" +
                    "  name TEXT NOT NULL,\n" +
                    "  created_at TIMESTAMP default current_timestamp NOT NULL\n" +
                    "  )")
                    .execute();

            handle.createStatement("CREATE TABLE IF NOT EXISTS web_user_project (" +
                    "  id SERIAL PRIMARY KEY,\n" +
                    "  user_id INTEGER REFERENCES web_user(id),\n" +
                    "  project TEXT NOT NULL UNIQUE," +
                    "  scope_expression TEXT NOT NULL," +
                    "  write_key TEXT NOT NULL," +
                    "  read_key TEXT NOT NULL," +
                    "  is_admin BOOLEAN DEFAULT false + NOT NUL\nL" +
                    "  )")
                    .execute();
        }
    }

    public void createUser() {

    }


    public void getUser(String email, String password) {

    }
}

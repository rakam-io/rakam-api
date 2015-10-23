package org.rakam.ui;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.lambdaworks.crypto.SCryptUtil;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.event.metastore.Metastore;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.LongMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class WebUserService {

    private final DBI dbi;
    private final Metastore metastore;

    @Inject
    public WebUserService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource, Metastore metastore) {
        dbi = new DBI(dataSource);
        this.metastore = metastore;
        setup();
    }

    private void setup() {
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
                    "  project TEXT NOT NULL UNIQUE,\n" +
                    "  scope_expression TEXT NOT NULL,\n" +
                    "  has_read_permission BOOLEAN NOT NULL,\n" +
                    "  has_write_permission BOOLEAN NOT NULL,\n" +
                    "  is_admin BOOLEAN DEFAULT false NOT NULL" +
                    "  )")
                    .execute();
        }
    }

    public void createUser(String email, String password, String name) {
        final String scrypt = SCryptUtil.scrypt(password, 2 << 14, 8, 1);

        try(Handle handle = dbi.open()) {
            final long userId = handle
                    .createStatement("INSERT INTO web_user (email, password, name) VALUES (:email, :password, :name)")
                    .bind("email", email)
                    .bind("name", name)
                    .bind("password", scrypt).executeAndReturnGeneratedKeys(LongMapper.FIRST).first();

        }
    }


    public Optional<WebUser> getUser(String email, String password) {
        String hashedPassword;
        String name;

        Map<String, List<WebUser.UserApiKey>> projects;

        try(Handle handle = dbi.open()) {
            final Map<String, Object> data = handle
                    .createQuery("SELECT id, name, password FROM web_user WHERE email = :email")
                    .bind("email", email).first();
            if(data == null) {
                return Optional.empty();
            }
            hashedPassword = (String) data.get("password");
            name = (String) data.get("name");

            final List<Map<String, Object>> keys = handle.createQuery("SELECT id, project, scope_expression, has_read_permission, is_admin FROM web_user_project WHERE user_id = :userId")
                    .bind("userId", data.get("id"))
                    .list();

            projects = new HashMap<>();

            final List<Metastore.ProjectApiKeys> apiKeys = metastore
                    .getApiKeys(keys.stream().mapToInt(row -> (int) row.get("id")).toArray());

            for(Metastore.ProjectApiKeys apiKey : apiKeys) {
                final Map<String, Object> id = keys.stream().filter(key -> key.get("id").equals(apiKey.id)).findAny().get();
                if(id.get("scope_expression") != null) {
                    //TODO generate scoped key
                    throw new UnsupportedOperationException();
                }

                // TODO move this heavy operation outside of the connection scope.
                if(!SCryptUtil.check(password, hashedPassword)) {
                    return Optional.empty();
                }

                String masterKey = Boolean.TRUE.equals(id.get("is_admin")) ? apiKey.masterKey : null;
                String readKey = Boolean.TRUE.equals(id.get("has_read_permission")) ? apiKey.readKey : null;
                String writeKey = Boolean.TRUE.equals(id.get("has_write_permission")) ? apiKey.writeKey : null;
                projects.computeIfAbsent(apiKey.project, (k) -> Lists.newArrayList())
                        .add(new WebUser.UserApiKey(readKey, writeKey, masterKey));
            }
        }



        return Optional.of(new WebUser(email, name, projects));
    }



}

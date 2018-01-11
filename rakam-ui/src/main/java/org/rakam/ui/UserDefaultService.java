package org.rakam.ui;

import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.AbstractMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

//@Path("/ui/user-default")
//@IgnoreApi
public class UserDefaultService {
    @Inject
    public UserDefaultService() {
    }

    public Map<String, Object> getAll(Handle handle, @Named("user_id") UIPermissionParameterProvider.Project project) {
        return handle.createQuery("SELECT name, value FROM ui_user_defaults WHERE user_id = :user AND project_id = :project")
                .bind("project", project.project)
                .bind("user", project.userId)
                .map((ResultSetMapper<Map.Entry<String, Object>>) (index, r, ctx) ->
                        new AbstractMap.SimpleImmutableEntry<>(r.getString(1), JsonHelper.read(r.getString(2), Object.class)))
                .list().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    public <T> T get(Handle handle, @Named("user_id") UIPermissionParameterProvider.Project project, @ApiParam("name") String name) {
        return (T) handle.createQuery("SELECT value FROM ui_user_defaults WHERE user_id = :user AND project_id = :project AND name = :name")
                .bind("project", project.project)
                .bind("user", project.userId)
                .bind("name", name.toUpperCase(Locale.ENGLISH))
                .map((index, r, ctx) -> {
                    return JsonHelper.read(r.getString(1), Object.class);
                }).first();
    }

    @ProtectEndpoint(writeOperation = true)
    public void set(
            Handle handle,
            @Named("user_id") UIPermissionParameterProvider.Project project,
            @ApiParam("name") String name, @ApiParam("value") Object value) {
        try {
            handle.createStatement("INSERT INTO ui_user_defaults (project_id, user_id, name, value) VALUES (:project, :user, :name, :value)")
                    .bind("project", project.project)
                    .bind("user", project.userId)
                    .bind("name", name.toUpperCase(Locale.ENGLISH))
                    .bind("value", JsonHelper.encode(value)).execute();
        } catch (Exception e) {
            handle.createStatement("UPDATE ui_user_defaults SET value = :value WHERE project_id = :project AND user_id = :user AND name = :name")
                    .bind("project", project.project)
                    .bind("user", project.userId)
                    .bind("name", name.toUpperCase(Locale.ENGLISH))
                    .bind("value", JsonHelper.encode(value)).execute();
        }
    }
}

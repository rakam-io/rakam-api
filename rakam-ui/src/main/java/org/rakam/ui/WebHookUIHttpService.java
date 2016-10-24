package org.rakam.ui;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.util.List;
import java.util.Map;

@IgnoreApi
@Path("/ui/webhook")
@Api(value = "/ui/webhook")
public class WebHookUIHttpService
        extends HttpService
{
    private final DBI dbi;

    @Inject
    public WebHookUIHttpService(
            @com.google.inject.name.Named("ui.metadata.jdbc") JDBCPoolDataSource dataSource)
    {
        this.dbi = new DBI(dataSource);
    }

    @PostConstruct
    public void setup()
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS predefined_webhook (" +
                    "  name VARCHAR(255) NOT NULL," +
                    "  image TEXT NOT NULL," +
                    "  description TEXT NOT NULL," +
                    "  code TEXT," +
                    "  parameters TEXT," +
                    "  PRIMARY KEY (name)" +
                    "  )")
                    .execute();
        }
    }

    @GET
    @ApiOperation(value = "List webhooks", response = Integer.class)
    @Path("/list")
    public List<UIWebHook> list()
    {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT name, image, description, code, parameters FROM predefined_webhook")
                    .map((index, r, ctx) -> {
                        return new UIWebHook(r.getString(1), r.getString(2), r.getString(3), r.getString(4),
                                JsonHelper.read(r.getString(5), Map.class));
                    }).list();
        }
    }

    public static class WebHookParameter
    {
        public final String type;
        public final String placeholder;
        public final String description;

        @JsonCreator
        public WebHookParameter(
                @ApiParam("type") String type,
                @ApiParam("placeholder") String placeholder,
                @ApiParam("description") String description)
        {
            this.type = type;
            this.placeholder = placeholder;
            this.description = description;
        }
    }

    public static class UIWebHook
    {
        public final String name;
        public final String image;
        public final String description;
        public final String code;
        public final Map<String, WebHookParameter> parameters;

        @JsonCreator
        public UIWebHook(@ApiParam("name") String name,
                @ApiParam("image") String image,
                @ApiParam("description") String description,
                @ApiParam("code") String code,
                @ApiParam("parameters") Map<String, WebHookParameter> parameters)
        {
            this.name = name;
            this.image = image;
            this.description = description;
            this.code = code;
            this.parameters = parameters;
        }
    }
}

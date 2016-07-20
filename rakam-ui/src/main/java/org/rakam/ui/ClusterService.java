package org.rakam.ui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Throwables;
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.config.EncryptionConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.CookieParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.JsonHelper;
import org.rakam.util.SuccessMessage;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.StringMapper;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.SocketException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.rakam.ui.user.WebUserHttpService.extractUserFromCookie;

@Path("/ui/cluster")
@IgnoreApi
public class ClusterService
        extends HttpService
{
    private final DBI dbi;
    private final EncryptionConfig encryptionConfig;

    @Inject
    public ClusterService(@Named("ui.metadata.jdbc") JDBCPoolDataSource dataSource, EncryptionConfig encryptionConfig)
    {
        dbi = new DBI(dataSource);
        this.encryptionConfig = encryptionConfig;
    }

    @JsonRequest
    @ApiOperation(value = "Register cluster", authorizations = @Authorization(value = "read_key"))
    @Path("/register")
    public SuccessMessage register(@CookieParam("session") String session,
            @BodyParam Cluster cluster)
    {
        int id = extractUserFromCookie(session, encryptionConfig.getSecretKey());

        boolean unreachable;
        try {
            HttpURLConnection con = (HttpURLConnection) new URL(cluster.apiUrl + "/admin/lock_key")
                    .openConnection();
            con.setRequestMethod("POST");

            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            HashMap<Object, Object> obj = new HashMap<>();
            obj.put("lock_key", cluster.lockKey);
            wr.write(JsonHelper.encodeAsBytes(obj));
            wr.flush();
            wr.close();

            unreachable = con.getResponseCode() == 0;

            if (!unreachable) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                boolean read;
                try {
                    read = JsonHelper.read(response.toString(), Boolean.class);
                }
                catch (Exception e) {
                    throw new RakamException("The API returned invalid response. Not a Rakam API?", BAD_REQUEST);
                }

                if (!read) {
                    throw new RakamException("Lock key is invalid.", FORBIDDEN);
                }
            }
        }
        catch (SocketException e) {
            unreachable = true;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        try (Handle handle = dbi.open()) {
            try {
                handle.createStatement("INSERT INTO rakam_cluster (user_id, api_url, lock_key) VALUES (:userId, :apiUrl, :lockKey)")
                        .bind("userId", id)
                        .bind("apiUrl", cluster.apiUrl)
                        .bind("lockKey", cluster.lockKey).execute();
            }
            catch (Exception e) {
                if (handle.createQuery("SELECT 1 FROM rakam_cluster WHERE (user_id, api_url, lock_key) = (:userId, :apiUrl, :lockKey)")
                        .bind("apiUrl", cluster.apiUrl)
                        .bind("lockKey", cluster.lockKey).bind("userId", id).first() != null) {
                    throw new AlreadyExistsException("Dashboard", BAD_REQUEST);
                }

                throw e;
            }

            return !unreachable ?
                    SuccessMessage.success("The API is unreachable.") :
                    SuccessMessage.success();
        }
    }

    @JsonRequest
    @ApiOperation(value = "Delete cluster", authorizations = @Authorization(value = "read_key"))
    @Path("/get")
    public SuccessMessage delete(@CookieParam("session") String session,
            @ApiParam("api_url") String apiUrl)
    {
        int id = extractUserFromCookie(session, encryptionConfig.getSecretKey());

        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM rakam_cluster WHERE (user_id, api_url) VALUES (:userId, :apiUrl)")
                    .bind("userId", id)
                    .bind("apiUrl", apiUrl).execute();
            return SuccessMessage.success();
        }
    }

    @JsonRequest
    @ApiOperation(value = "List cluster", authorizations = @Authorization(value = "read_key"))
    @Path("/list")
    @GET
    public List<String> list(@CookieParam("session") String session)
    {
        int id = extractUserFromCookie(session, encryptionConfig.getSecretKey());

        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT api_url FROM rakam_cluster WHERE user_id = :userId")
                    .bind("userId", id).map(StringMapper.FIRST).list();
        }
    }

    public static class Cluster
    {
        public final String apiUrl;
        public final String lockKey;

        @JsonCreator
        public Cluster(@ApiParam("api_url") String apiUrl, @ApiParam("lock_key") String lockKey)
        {
            this.apiUrl = apiUrl;
            this.lockKey = lockKey;
        }
    }
}

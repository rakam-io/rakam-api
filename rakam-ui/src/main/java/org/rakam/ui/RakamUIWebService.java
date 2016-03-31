package org.rakam.ui;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.DefaultFileRegion;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import net.kencochrane.raven.jul.SentryHandler;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.ui.ActiveModuleListBuilder.ActiveModuleList;
import org.rakam.util.IgnorePermissionCheck;
import org.rakam.util.JsonHelper;

import javax.activation.MimetypesFileTypeMap;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.logging.LogManager;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


@Path("/")
public class RakamUIWebService extends HttpService {
    public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
    public static final String HTTP_DATE_GMT_TIMEZONE = "GMT";
    public static final int HTTP_CACHE_SECONDS = 60 * 60 * 24;
    private final File directory;
    private final ActiveModuleList activeModules;

    @Inject
    public RakamUIWebService(RakamUIConfig config, ActiveModuleListBuilder activeModuleListBuilder) {
        activeModules = activeModuleListBuilder.build();
        directory = config.getUIDirectory();
    }

    @Path("/favicon.ico")
    @GET
    @IgnorePermissionCheck
    public void favicon(RakamHttpRequest request) {
        sendFile(request, new File(directory.getPath(), "favicon.ico"));
    }

    @Path("/check-sentry")
    @GET
    @IgnorePermissionCheck
    public void checkSentry(RakamHttpRequest request) {
        LogManager manager = LogManager.getLogManager();
        String dsnInternal = manager.getProperty(SentryHandler.class.getCanonicalName() + ".dsn");
        String tagsString = manager.getProperty(SentryHandler.class.getCanonicalName() + ".tags");
        String dsnPublic = Optional.ofNullable(dsnInternal).map(urlString -> {
            try {
                URL url = new URL(urlString);
                String[] userPass = url.getUserInfo().split(":", 2);
                // Use public DNS
                return url.getProtocol() + "://" + (userPass.length > 0 ? userPass[0] : "" + url) +
                        "@" + url.getHost() + url.getPath() + "?" + url.getQuery();
            } catch (MalformedURLException e) {
                return null;
            }
        }).orElse(null);

        Map<String, String> tags = Optional.ofNullable(tagsString).map(str ->
                Arrays.stream(str.split(",")).map(val -> val.split(":")).collect(Collectors.toMap(a -> a[0], a -> a[1])))
                .orElse(null);
        tags.remove("type");

        request.response(JsonHelper.encode(ImmutableMap.of("tags", tags, "dsn", dsnPublic)), OK).end();
    }

    @Path("/ui/active-modules")
    @GET
    @ApiOperation(value = "List installed modules for ui",
            authorizations = @Authorization(value = "master_key")
    )
    @IgnorePermissionCheck
    public ActiveModuleList modules() {
        return activeModules;
    }

    private void sendFile(RakamHttpRequest request, File file) {
        if (file.isHidden() || !file.exists()) {
            sendError(request, NOT_FOUND);
            return;
        }

        if (!file.isFile()) {
            sendError(request, FORBIDDEN);
            return;
        }

        // Cache Validation
        String ifModifiedSince = request.headers().get(IF_MODIFIED_SINCE);
        if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
            SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
            Date ifModifiedSinceDate;
            try {
                ifModifiedSinceDate = dateFormatter.parse(ifModifiedSince);
            } catch (ParseException e) {
                sendError(request, HttpResponseStatus.BAD_REQUEST);
                return;
            }

            // Only compare up to the second because the datetime format we send to the client
            // does not have milliseconds
            long ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000;
            long fileLastModifiedSeconds = file.lastModified() / 1000;
            if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
                sendNotModified(request);
                return;
            }
        }

        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException ignore) {
            sendError(request, NOT_FOUND);
            return;
        }

        long fileLength;
        try {
            fileLength = raf.length();
        } catch (IOException e) {
            sendError(request, HttpResponseStatus.BAD_GATEWAY);
            return;
        }

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        HttpHeaders.setContentLength(response, fileLength);
        setContentTypeHeader(response, file);
        setDateAndCacheHeaders(response, file);
        if (HttpHeaders.isKeepAlive(request)) {
            response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        }

        request.context().write(response);

        DefaultFileRegion defaultFileRegion = new DefaultFileRegion(raf.getChannel(), 0, fileLength);
        request.context().write(defaultFileRegion, request.context().newProgressivePromise());
        ChannelFuture lastContentFuture = request.context().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

        if (!HttpHeaders.isKeepAlive(request)) {
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Path("/*")
    @GET
    @IgnorePermissionCheck
    public void main(RakamHttpRequest request) {
        if (!request.getDecoderResult().isSuccess()) {
            sendError(request, BAD_REQUEST);
            return;
        }

        if (request.getMethod() != GET) {
            sendError(request, METHOD_NOT_ALLOWED);
            return;
        }

        final String uri = request.path();

        int idx = uri.indexOf("/static/");
        File file;
        if (idx > -1) {
            final String path = sanitizeUri(uri);
            if (path == null) {
                sendError(request, FORBIDDEN);
                return;
            }
            file = new File(path);
        } else {
            file = new File(directory.getPath(), "index.html");
        }

        sendFile(request, file);
    }


    private static final Pattern INSECURE_URI = Pattern.compile(".*[<>&\"].*");

    private String sanitizeUri(String uri) {
        try {
            uri = URLDecoder.decode(uri, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new Error(e);
        }

        if (uri.isEmpty() || uri.charAt(0) != '/') {
            return null;
        }

        uri = uri.replace('/', File.separatorChar);

        // Simplistic dumb security check.
        // You will have to do something serious in the production environment.
        if (uri.contains(File.separator + '.') ||
                uri.contains('.' + File.separator) ||
                uri.charAt(0) == '.' || uri.charAt(uri.length() - 1) == '.' ||
                INSECURE_URI.matcher(uri).matches()) {
            return null;
        }

        return directory.getPath() + File.separator + uri;
    }

    private static void sendError(RakamHttpRequest request, HttpResponseStatus status) {
        request.response(status.reasonPhrase(), status).end();
    }

    private static void sendNotModified(RakamHttpRequest request) {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, NOT_MODIFIED);

        SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

        Calendar time = new GregorianCalendar();
        response.headers().set(DATE, dateFormatter.format(time.getTime()));

        request.response(response).end();
    }

    static void setDateAndCacheHeaders(HttpResponse response, File fileToCache) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

        Calendar time = new GregorianCalendar();
        response.headers().set(DATE, dateFormatter.format(time.getTime()));

        time.add(Calendar.SECOND, HTTP_CACHE_SECONDS);
        response.headers().set(EXPIRES, dateFormatter.format(time.getTime()));
        response.headers().set(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        response.headers().set(CACHE_CONTROL, "private, max-age=" + HTTP_CACHE_SECONDS);
        response.headers().set(LAST_MODIFIED, dateFormatter.format(new Date(fileToCache.lastModified())));
    }

    static void setContentTypeHeader(HttpResponse response, File file) {
        MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();
        response.headers().set(CONTENT_TYPE, mimeTypesMap.getContentType(file.getPath()));
    }

}

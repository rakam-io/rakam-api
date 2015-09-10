package org.rakam.ui;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
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
import org.rakam.plugin.EventExplorerConfig;
import org.rakam.plugin.EventStreamConfig;
import org.rakam.plugin.RealTimeConfig;
import org.rakam.plugin.UserPluginConfig;
import org.rakam.plugin.UserStorage;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;

import javax.activation.MimetypesFileTypeMap;
import javax.ws.rs.Path;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Optional;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static io.netty.handler.codec.http.HttpHeaders.Names.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.DATE;
import static io.netty.handler.codec.http.HttpHeaders.Names.EXPIRES;
import static io.netty.handler.codec.http.HttpHeaders.Names.IF_MODIFIED_SINCE;
import static io.netty.handler.codec.http.HttpHeaders.Names.LAST_MODIFIED;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Created by buremba <Burak Emre Kabakcı> on 09/05/15 00:49.
 */
@Path("/")
public class RakamUIWebService extends HttpService {
    public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
    public static final String HTTP_DATE_GMT_TIMEZONE = "GMT";
    public static final int HTTP_CACHE_SECONDS = 60;
    private final File directory;
    private final ActiveModuleList activeModules;

    @Inject
    public RakamUIWebService(RakamUIModule.RakamUIConfig config, ActiveModuleListBuilder activeModuleListBuilder) {
        URI uri;
        try {
            uri = new URI(config.getUI());
        } catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }
        activeModules = activeModuleListBuilder.build();
        directory = new File(new File(uri.getHost(), uri.getPath()), Optional.ofNullable(config.getDirectory()).orElse("/"));
    }

    private static class ActiveModuleListBuilder {
        private final UserPluginConfig userPluginConfig;
        private final RealTimeConfig realtimeConfig;
        private final EventStreamConfig eventStreamConfig;
        private final EventExplorerConfig eventExplorerConfig;
        private final UserStorage userStorage;

        @Inject
        public ActiveModuleListBuilder(UserPluginConfig userPluginConfig, RealTimeConfig realtimeConfig, EventStreamConfig eventStreamConfig, EventExplorerConfig eventExplorerConfig, UserStorage userStorage) {
           this.userPluginConfig = userPluginConfig;
           this.realtimeConfig = realtimeConfig;
           this.eventStreamConfig = eventStreamConfig;
           this.eventExplorerConfig = eventExplorerConfig;
           this.userStorage = userStorage;
        }

        public ActiveModuleList build() {
            return new ActiveModuleList(userPluginConfig, realtimeConfig, eventStreamConfig, eventExplorerConfig, userStorage);
        }
    }
    private static class ActiveModuleList {
        @JsonProperty
        private final boolean userStorage;
        @JsonProperty
        private final boolean userMailbox;
        @JsonProperty
        private final boolean funnelAnalysisEnabled;
        @JsonProperty
        private final boolean retentionAnalysisEnabled;
        @JsonProperty
        private final boolean eventExplorer;
        @JsonProperty
        private final boolean realtime;
        @JsonProperty
        private final boolean eventStream;
        @JsonProperty
        private final boolean userStorageEventFilter;

        private ActiveModuleList(UserPluginConfig userPluginConfig, RealTimeConfig realtimeConfig, EventStreamConfig eventStreamConfig, EventExplorerConfig eventExplorerConfig, UserStorage userStorage) {
            this.userStorage = userPluginConfig.getStorageModule() != null;
            this.userMailbox = userPluginConfig.getMailBoxStorageModule() != null;
            this.funnelAnalysisEnabled = userPluginConfig.isFunnelAnalysisEnabled();
            this.retentionAnalysisEnabled = userPluginConfig.isRetentionAnalysisEnabled();
            this.eventExplorer = eventExplorerConfig.isEventExplorerEnabled();
            this.realtime = realtimeConfig.isRealtimeModuleEnabled();
            this.eventStream = eventStreamConfig.isEventStreamEnabled();
            this.userStorageEventFilter = userStorage.isEventFilterSupported();
        }
    }

    @Path("/ui/active-modules")
    @javax.ws.rs.GET
    public ActiveModuleList modules() {
        return activeModules;
    }

    @Path("/*")
    @javax.ws.rs.GET
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
        String path;
        if(idx > -1) {
            path = sanitizeUri(uri);
            if (path == null) {
                sendError(request, FORBIDDEN);
                return;
            }
        }else {
            path = directory.getPath() + File.separator + "index.html";
        }

        File file = new File(path);
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
        response.headers().set(CACHE_CONTROL, "private, max-age=" + HTTP_CACHE_SECONDS);
        response.headers().set(LAST_MODIFIED, dateFormatter.format(new Date(fileToCache.lastModified())));
    }

    static void setContentTypeHeader(HttpResponse response, File file) {
        MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();
        response.headers().set(CONTENT_TYPE, mimeTypesMap.getContentType(file.getPath()));
    }

}

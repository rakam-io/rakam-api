package org.rakam.server;

import com.facebook.presto.jdbc.internal.guava.base.Preconditions;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.HttpRequestHandler;

import java.util.HashMap;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/07/14 06:03.
 */
public class RouteMatcher {
    HashMap<PatternBinding, HttpRequestHandler> routes = new HashMap();
    private HttpRequestHandler noMatch = request -> request.response("404", HttpResponseStatus.OK).end();

    public void handle(RakamHttpRequest request) {
        String path = request.path();
        int lastIndex = path.length() - 1;
        if(path.charAt(lastIndex) == '/')
            path = path.substring(0, lastIndex);

        final HttpRequestHandler httpRequestHandler = routes.get(new PatternBinding(request.getMethod(), path));
        if (httpRequestHandler != null) {
            httpRequestHandler.handle(request);
        } else {
            noMatch.handle(request);
        }
    }

    public void add(HttpMethod method, String pattern, HttpRequestHandler handler) {
        routes.put(new PatternBinding(method, pattern), handler);
    }

    public void remove(HttpMethod method, String pattern, HttpRequestHandler handler) {
        routes.remove(new PatternBinding(method, pattern));
    }

    public void noMatch(HttpRequestHandler handler) {
        noMatch = handler;
    }

    public static class PatternBinding {
        final HttpMethod method;
        final String pattern;

        private PatternBinding(HttpMethod method, String pattern) {
            this.method = method;
            this.pattern = pattern;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PatternBinding)) return false;

            PatternBinding that = (PatternBinding) o;

            if (!method.equals(that.method)) return false;
            if (!pattern.equals(that.pattern)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = method.hashCode();
            result = 31 * result + pattern.hashCode();
            return result;
        }
    }

    public static class MicroRouteMatcher {
        private final RouteMatcher routeMatcher;
        private String path;

        public MicroRouteMatcher(RouteMatcher routeMatcher, String path) {
            this.routeMatcher = routeMatcher;
            this.path = path;
        }

        public void add(String lastPath, HttpMethod method, HttpRequestHandler handler) {
            Preconditions.checkNotNull(path, "path is not configured");
            routeMatcher.add(method, path + lastPath, handler);
        }
    }
}
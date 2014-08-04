package org.rakam.server;

import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/07/14 06:03.
 */
public class RouteMatcher implements Handler<HttpServerRequest> {

    private final List<PatternBinding> getBindings = new ArrayList<>();
    private final List<PatternBinding> putBindings = new ArrayList<>();
    private final List<PatternBinding> postBindings = new ArrayList<>();
    private final List<PatternBinding> deleteBindings = new ArrayList<>();
    private final List<PatternBinding> optionsBindings = new ArrayList<>();
    private final List<PatternBinding> headBindings = new ArrayList<>();
    private final List<PatternBinding> connectBindings = new ArrayList<>();
    private Handler<HttpServerRequest> noMatchHandler;

    @Override
    public void handle(HttpServerRequest request) {
        request.response().putHeader("Content-Type", "application/json; charset=utf-8");
        request.response().putHeader("Access-Control-Allow-Origin", "*");
        request.response().putHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");

        switch (request.method()) {
            case "POST":
                route(request, postBindings);
                break;
            case "GET":
                route(request, getBindings);
                break;
            case "PUT":
                route(request, putBindings);
                break;
            case "DELETE":
                route(request, deleteBindings);
                break;
            case "OPTIONS":
                route(request, optionsBindings);
                break;
            case "HEAD":
                route(request, headBindings);
                break;
            case "CONNECT":
                route(request, connectBindings);
                break;
            default:
                notFound(request);
        }
    }

    public RouteMatcher get(String pattern, Handler<HttpServerRequest> handler) {
        addPattern(pattern, handler, getBindings);
        return this;
    }

    public RouteMatcher getStartsWith(String pattern, Handler<HttpServerRequest> handler) {
        addPattern(pattern, true, handler, getBindings);
        return this;
    }

    public RouteMatcher put(String pattern, Handler<HttpServerRequest> handler) {
        addPattern(pattern, handler, putBindings);
        return this;
    }

    public RouteMatcher putStartsWith(String pattern, Handler<HttpServerRequest> handler) {
        addPattern(pattern, true, handler, putBindings);
        return this;
    }

    public RouteMatcher post(String pattern, Handler<HttpServerRequest> handler) {
        addPattern(pattern, handler, postBindings);
        return this;
    }

    public RouteMatcher postStartsWith(String pattern, Handler<HttpServerRequest> handler) {
        addPattern(pattern, true, handler, postBindings);
        return this;
    }

    public RouteMatcher delete(String pattern, Handler<HttpServerRequest> handler) {
        addPattern(pattern, handler, deleteBindings);
        return this;
    }

    public RouteMatcher options(String pattern, Handler<HttpServerRequest> handler) {
        addPattern(pattern, handler, optionsBindings);
        return this;
    }

    public RouteMatcher head(String pattern, Handler<HttpServerRequest> handler) {
        addPattern(pattern, handler, headBindings);
        return this;
    }

    public RouteMatcher connect(String pattern, Handler<HttpServerRequest> handler) {
        addPattern(pattern, handler, connectBindings);
        return this;
    }

    public RouteMatcher all(String pattern, Handler<HttpServerRequest> handler) {
        addPattern(pattern, handler, getBindings);
        addPattern(pattern, handler, putBindings);
        addPattern(pattern, handler, postBindings);
        addPattern(pattern, handler, deleteBindings);
        addPattern(pattern, handler, optionsBindings);
        addPattern(pattern, handler, headBindings);
        addPattern(pattern, handler, connectBindings);
        return this;
    }

    public RouteMatcher noMatch(Handler<HttpServerRequest> handler) {
        noMatchHandler = handler;
        return this;
    }

    private static void addPattern(String input, Handler<HttpServerRequest> handler, List<PatternBinding> bindings) {
        PatternBinding binding = new PatternBinding(input, handler);
        bindings.add(binding);
    }

    private static void addPattern(String input, boolean startsWith, Handler<HttpServerRequest> handler, List<PatternBinding> bindings) {
        PatternBinding binding = new PatternBinding(input, startsWith, handler);
        bindings.add(binding);
    }

    private void route(HttpServerRequest request, List<PatternBinding> bindings) {
        for (PatternBinding binding: bindings) {
            if((!binding.startsWith && binding.pattern.equals(request.path())) ||
                    (binding.startsWith && request.path().startsWith(binding.pattern))) {
                binding.handler.handle(request);
                return;
            }
        }
        notFound(request);
    }

    private void notFound(HttpServerRequest request) {
        if (noMatchHandler != null) {
            noMatchHandler.handle(request);
        } else {
            request.response().setStatusCode(404);
            request.response().end();
        }
    }

    private static class PatternBinding {
        final String pattern;
        final Handler<HttpServerRequest> handler;
        final boolean startsWith;

        private PatternBinding(String pattern, boolean startsWith, Handler<HttpServerRequest> handler) {
            this.pattern = pattern;
            this.handler = handler;
            this.startsWith = startsWith;
        }

        private PatternBinding(String pattern, Handler<HttpServerRequest> handler) {
            this.pattern = pattern;
            this.handler = handler;
            this.startsWith = false;
        }
    }

}
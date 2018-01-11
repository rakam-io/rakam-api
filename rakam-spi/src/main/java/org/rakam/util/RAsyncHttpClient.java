package org.rakam.util;

import com.google.common.io.ByteStreams;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import okhttp3.*;
import org.asynchttpclient.Response;
import org.asynchttpclient.cookie.Cookie;
import org.asynchttpclient.uri.Uri;
import org.asynchttpclient.util.HttpConstants.Methods;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.SocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RAsyncHttpClient {
    private final static Logger LOGGER = Logger.get(RAsyncHttpClient.class);

    private final OkHttpClient asyncHttpClient;

    public RAsyncHttpClient(OkHttpClient asyncHttpClient) {
        this.asyncHttpClient = asyncHttpClient;
    }

    public static RAsyncHttpClient create(int timeoutInMillis, String userAgent) {
        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(timeoutInMillis, TimeUnit.MILLISECONDS)
                .readTimeout(timeoutInMillis, TimeUnit.MILLISECONDS)
                .writeTimeout(timeoutInMillis, TimeUnit.MILLISECONDS)
                .build();

        return new RAsyncHttpClient(client);
    }

    public NashornHttpRequest get(String url) {
        return request(Methods.GET, url);
    }

    public NashornHttpRequest delete(String url) {
        return request(Methods.DELETE, url);
    }

    public NashornHttpRequest post(String url, String body) {
        return request(Methods.POST, url).data(body);
    }

    public NashornHttpRequest post(String url) {
        return request(Methods.POST, url);
    }

    public NashornHttpRequest put(String url, String body) {
        return request(Methods.PUT, url).data(body);
    }

    public NashornHttpRequest request(String method, String url) {
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("URL is not set: " + url);
        }

        return new NashornHttpRequest(new Request.Builder(), method, url);
    }

    public class NashornHttpRequest {
        private final Request.Builder requestBuilder;
        private final String method;
        private HttpUrl url;
        private FormBody.Builder formParams;

        public NashornHttpRequest(Request.Builder requestBuilder, String method, String url) {
            this.requestBuilder = requestBuilder;
            this.method = method;
            try {
                this.url = HttpUrl.get(new URL(url));
            } catch (MalformedURLException e) {
                throw new RakamException("URL is not valid", HttpResponseStatus.BAD_REQUEST);
            }
        }

        public synchronized NashornHttpRequest form(String key, String value) {
            if (formParams == null) {
                requestBuilder.addHeader("Content-type", "application/x-www-form-urlencoded");
                formParams = new FormBody.Builder();
            }

            formParams.add(key, value);
            return this;
        }

        public NashornHttpRequest data(String data) {
            RequestBody body = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), data);
            requestBuilder.method(method, body);
            return this;
        }

        public NashornHttpRequest header(String key, String value) {
            if (value != null) {
                requestBuilder.addHeader(key, value);
            }
            return this;
        }

        public NashornHttpRequest query(String key, String value) {
            url = url.newBuilder().addQueryParameter(key, value).build();
            return this;
        }

//        public NashornHttpRequest cookie(String key, String value, String domain, String path, boolean secure, boolean httpOnly)
//        {
//            requestBuilder.addCookie(Cookie.newValidCookie(key, value, true, domain, path, System.currentTimeMillis() + 1000, secure, httpOnly));
//            return this;
//        }
//
//        public NashornHttpRequest cookie(String key, String value)
//        {
//            requestBuilder.addCookie(Cookie.newValidCookie(key, value, true, null, null, System.currentTimeMillis() + 1000, true, true));
//            return this;
//        }

        public Response send() {
            requestBuilder.url(url);
            if (formParams != null) {
                requestBuilder.method(method, formParams.build());
            }
            try {
                Call call = asyncHttpClient.newCall(requestBuilder.build());
                okhttp3.Response response = call.execute();
                LOGGER.debug("Performed request to %s in %dms", url, response.receivedResponseAtMillis() - response.sentRequestAtMillis());
                return new SuccessResponse(response);
            } catch (IOException e) {
                return new ExceptionResponse(e);
            }
        }

        private class ExceptionResponse
                implements Response {
            private final Throwable ex;

            public ExceptionResponse(Throwable ex) {
                this.ex = ex;
            }

            @Override
            public int getStatusCode() {
                return 0;
            }

            @Override
            public String getStatusText() {
                return ex.getMessage();
            }

            @Override
            public byte[] getResponseBodyAsBytes() {
                return ex.getMessage().getBytes(UTF_8);
            }

            @Override
            public ByteBuffer getResponseBodyAsByteBuffer() {
                return ByteBuffer.wrap(ex.getMessage().getBytes(UTF_8));
            }

            @Override
            public InputStream getResponseBodyAsStream() {
                return new ByteArrayInputStream(ex.getMessage().getBytes(UTF_8));
            }

            @Override
            public String getResponseBody(Charset charset) {
                return ex.getMessage();
            }

            @Override
            public String getResponseBody() {
                return ex.getMessage();
            }

            @Override
            public Uri getUri() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getContentType() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getHeader(String s) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<String> getHeaders(String s) {
                throw new UnsupportedOperationException();
            }

            @Override
            public HttpHeaders getHeaders() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isRedirected() {
                return false;
            }

            @Override
            public List<Cookie> getCookies() {
                return null;
            }

            @Override
            public boolean hasResponseStatus() {
                return false;
            }

            @Override
            public boolean hasResponseHeaders() {
                return false;
            }

            @Override
            public boolean hasResponseBody() {
                return false;
            }

            @Override
            public SocketAddress getRemoteAddress() {
                return null;
            }

            @Override
            public SocketAddress getLocalAddress() {
                return null;
            }
        }
    }

    private class SuccessResponse
            implements Response {
        private final okhttp3.Response response;

        public SuccessResponse(okhttp3.Response response) {
            this.response = response;
        }

        @Override
        public int getStatusCode() {
            return response.code();
        }

        @Override
        public String getStatusText() {
            return HttpResponseStatus.valueOf(response.code()).reasonPhrase();
        }

        @Override
        public byte[] getResponseBodyAsBytes() {
            try {
                ResponseBody body = response.body();
                if ("gzip".equals(response.header("Content-Encoding"))) {
                    return ByteStreams.toByteArray(new GZIPInputStream(body.byteStream()));
                } else {
                    return body.bytes();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public ByteBuffer getResponseBodyAsByteBuffer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream getResponseBodyAsStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getResponseBody(Charset charset) {
            return new String(getResponseBodyAsBytes(), charset);
        }

        @Override
        public String getResponseBody() {
            return getResponseBody(StandardCharsets.UTF_8);
        }

        @Override
        public Uri getUri() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getContentType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getHeader(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getHeaders(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpHeaders getHeaders() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isRedirected() {
            return false;
        }

        @Override
        public List<Cookie> getCookies() {
            return null;
        }

        @Override
        public boolean hasResponseStatus() {
            return false;
        }

        @Override
        public boolean hasResponseHeaders() {
            return false;
        }

        @Override
        public boolean hasResponseBody() {
            return false;
        }

        @Override
        public SocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public SocketAddress getLocalAddress() {
            return null;
        }
    }
}
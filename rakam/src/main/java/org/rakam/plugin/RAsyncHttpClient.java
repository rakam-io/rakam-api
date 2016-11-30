package org.rakam.plugin;

import io.netty.channel.epoll.Epoll;
import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.Param;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.asynchttpclient.cookie.Cookie;
import org.asynchttpclient.uri.Uri;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.asynchttpclient.util.HttpConstants.Methods.DELETE;
import static org.asynchttpclient.util.HttpConstants.Methods.GET;
import static org.asynchttpclient.util.HttpConstants.Methods.POST;
import static org.asynchttpclient.util.HttpConstants.Methods.PUT;

public class RAsyncHttpClient
{
    private final AsyncHttpClient asyncHttpClient;

    public RAsyncHttpClient(AsyncHttpClient asyncHttpClient)
    {
        this.asyncHttpClient = asyncHttpClient;
    }

    public static RAsyncHttpClient create(int timeoutInMillis, String userAgent)
    {
        AsyncHttpClientConfig cf = new DefaultAsyncHttpClientConfig.Builder()
                .setRequestTimeout(timeoutInMillis)
                .setUserAgent(userAgent)
                .setUseNativeTransport(Epoll.isAvailable())
                .build();

        org.asynchttpclient.AsyncHttpClient asyncHttpClient = new DefaultAsyncHttpClient(cf);
        return new RAsyncHttpClient(asyncHttpClient);
    }

    public NashornHttpRequest get(String url)
    {
        return new NashornHttpRequest(new RequestBuilder(GET).setUrl(url));
    }

    public NashornHttpRequest delete(String url)
    {
        return new NashornHttpRequest(new RequestBuilder(DELETE).setUrl(url));
    }

    public NashornHttpRequest post(String url, String body)
    {
        return new NashornHttpRequest(new RequestBuilder(POST).setUrl(url).setBody(body));
    }

    public NashornHttpRequest put(String url, String body)
    {
        return new NashornHttpRequest(new RequestBuilder(PUT).setUrl(url).setBody(body));
    }

    public NashornHttpRequest request(String type, String url)
    {
        return new NashornHttpRequest(new RequestBuilder(type).setUrl(url));
    }

    public class NashornHttpRequest
    {
        private final RequestBuilder requestBuilder;
        private CompletableFuture<Response> future;

        public NashornHttpRequest(RequestBuilder requestBuilder)
        {
            this.requestBuilder = requestBuilder;
        }

        public NashornHttpRequest form(List<Param> formData)
        {
            requestBuilder.setFormParams(formData);
            return this;
        }

        public NashornHttpRequest data(String data)
        {
            requestBuilder.setBody(data);
            return this;
        }

        public NashornHttpRequest header(String key, String value)
        {
            requestBuilder.addHeader(key, value);
            return this;
        }

        public NashornHttpRequest cookie(String key, String value, String domain, String path, boolean secure, boolean httpOnly)
        {
            requestBuilder.addCookie(Cookie.newValidCookie(key, value, true, domain, path, System.currentTimeMillis() + 1000, secure, httpOnly));
            return this;
        }

        public NashornHttpRequest cookie(String key, String value)
        {
            requestBuilder.addCookie(Cookie.newValidCookie(key, value, true, null, null, System.currentTimeMillis() + 1000, true, true));
            return this;
        }

        public Response send()
        {
            try {
                return asyncHttpClient.prepareRequest(requestBuilder).execute().get();
            }
            catch (InterruptedException | ExecutionException e) {
                return new ExceptionResponse(e);
            }
        }

        private NashornHttpRequest then(Function<Response, Object> successConsumer, Function<Response, Object> errorConsumer)
        {
            if (future == null) {
                future = asyncHttpClient.prepareRequest(requestBuilder).execute().toCompletableFuture();
            }

            future = future.whenComplete((response, ex) -> {
                if (ex != null) {
                    Response t = new ExceptionResponse(ex);
                    errorConsumer.apply(t);
                }
                else {
                    if (response.getStatusCode() == 200) {
                        successConsumer.apply(response);
                    }
                    else {
                        errorConsumer.apply(response);
                    }
                }
            });

            return this;
        }

        private class ExceptionResponse
                implements Response
        {
            private final Throwable ex;

            public ExceptionResponse(Throwable ex) {this.ex = ex;}

            @Override
            public int getStatusCode()
            {
                return 0;
            }

            @Override
            public String getStatusText()
            {
                return ex.getMessage();
            }

            @Override
            public byte[] getResponseBodyAsBytes()
            {
                return ex.getMessage().getBytes(UTF_8);
            }

            @Override
            public ByteBuffer getResponseBodyAsByteBuffer()
            {
                return ByteBuffer.wrap(ex.getMessage().getBytes(UTF_8));
            }

            @Override
            public InputStream getResponseBodyAsStream()
            {
                return new ByteArrayInputStream(ex.getMessage().getBytes(UTF_8));
            }

            @Override
            public String getResponseBody(Charset charset)
            {
                return ex.getMessage();
            }

            @Override
            public String getResponseBody()
            {
                return ex.getMessage();
            }

            @Override
            public Uri getUri()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getContentType()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getHeader(String s)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<String> getHeaders(String s)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public HttpHeaders getHeaders()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isRedirected()
            {
                return false;
            }

            @Override
            public List<Cookie> getCookies()
            {
                return null;
            }

            @Override
            public boolean hasResponseStatus()
            {
                return false;
            }

            @Override
            public boolean hasResponseHeaders()
            {
                return false;
            }

            @Override
            public boolean hasResponseBody()
            {
                return false;
            }

            @Override
            public SocketAddress getRemoteAddress()
            {
                return null;
            }

            @Override
            public SocketAddress getLocalAddress()
            {
                return null;
            }
        }
    }
}
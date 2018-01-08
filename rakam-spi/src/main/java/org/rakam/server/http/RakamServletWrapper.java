package org.rakam.server.http;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.*;

public class RakamServletWrapper
        implements HttpServletRequest {

    private final RakamHttpRequest request;

    public RakamServletWrapper(RakamHttpRequest request) {
        this.request = request;
    }

    @Override
    public String getAuthType() {
        return null;
    }

    @Override
    public Cookie[] getCookies() {
        return null;
    }

    @Override
    public long getDateHeader(String s) {
        throw new IllegalStateException();
    }

    @Override
    public String getHeader(String s) {
        return request.headers().get(s);
    }

    @Override
    public Enumeration<String> getHeaders(String s) {
        return new Enumeration() {
            @Override
            public boolean hasMoreElements() {
                return false;
            }

            @Override
            public Object nextElement() {
                return request.headers().get(s);
            }
        };
    }

    @Override
    public Enumeration<String> getHeaderNames() {
        return new Vector(request.headers().names()).elements();
    }

    @Override
    public int getIntHeader(String s) {
        throw new IllegalStateException();
    }

    @Override
    public String getMethod() {
        return request.getMethod().name();
    }

    @Override
    public String getPathInfo() {
        return request.path();
    }

    @Override
    public String getPathTranslated() {
        return request.path();
    }

    @Override
    public String getContextPath() {
        return request.path();
    }

    @Override
    public String getQueryString() {
        try {
            return new URI(request.getUri()).getRawQuery();
        } catch (URISyntaxException e) {
            return request.getUri();
        }
    }

    @Override
    public String getRemoteUser() {
        return null;
    }

    @Override
    public boolean isUserInRole(String s) {
        return false;
    }

    @Override
    public Principal getUserPrincipal() {
        throw new IllegalStateException();
    }

    @Override
    public String getRequestedSessionId() {
        throw new IllegalStateException();
    }

    @Override
    public String getRequestURI() {
        return request.path();
    }

    @Override
    public StringBuffer getRequestURL() {
        return new StringBuffer(request.getUri());
    }

    @Override
    public String getServletPath() {
        throw new IllegalStateException();
    }

    @Override
    public HttpSession getSession(boolean b) {
        return null;
    }

    @Override
    public HttpSession getSession() {
        throw new IllegalStateException();
    }

    @Override
    public boolean isRequestedSessionIdValid() {
        throw new IllegalStateException();
    }

    @Override
    public boolean isRequestedSessionIdFromCookie() {
        throw new IllegalStateException();
    }

    @Override
    public boolean isRequestedSessionIdFromURL() {
        throw new IllegalStateException();
    }

    @Override
    public boolean isRequestedSessionIdFromUrl() {
        throw new IllegalStateException();
    }

    @Override
    public boolean authenticate(HttpServletResponse httpServletResponse)
            throws IOException, ServletException {
        throw new IllegalStateException();
    }

    @Override
    public void login(String s, String s1)
            throws ServletException {
        throw new IllegalStateException();
    }

    @Override
    public void logout()
            throws ServletException {
        throw new IllegalStateException();
    }

    @Override
    public Collection<Part> getParts()
            throws IOException, ServletException {
        throw new IllegalStateException();
    }

    @Override
    public Part getPart(String s)
            throws IOException, ServletException {
        throw new IllegalStateException();
    }

    @Override
    public Object getAttribute(String s) {
        throw new IllegalStateException();
    }

    @Override
    public Enumeration<String> getAttributeNames() {
        throw new IllegalStateException();
    }

    @Override
    public String getCharacterEncoding() {
        throw new IllegalStateException();
    }

    @Override
    public void setCharacterEncoding(String s)
            throws UnsupportedEncodingException {
        throw new IllegalStateException();
    }

    @Override
    public int getContentLength() {
        throw new IllegalStateException();
    }

    @Override
    public String getContentType() {
        throw new IllegalStateException();
    }

    @Override
    public ServletInputStream getInputStream()
            throws IOException {
        throw new IllegalStateException();
    }

    @Override
    public String getParameter(String s) {
        throw new IllegalStateException();
    }

    @Override
    public Enumeration<String> getParameterNames() {
        throw new IllegalStateException();
    }

    @Override
    public String[] getParameterValues(String s) {
        throw new IllegalStateException();
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        InputStream body = request.getBody();
        if (body == null) {
            return ImmutableMap.of();
        }

        String bodyStr;
        try {
            body.reset();
            bodyStr = CharStreams.toString(new InputStreamReader(ByteStreams.limit(body, 1000), Charsets.UTF_8));
        } catch (IOException e) {
            return ImmutableMap.of();
        }
        return ImmutableMap.of("raw", new String[]{bodyStr});
    }

    @Override
    public String getProtocol() {
        return request.getProtocolVersion().protocolName();
    }

    @Override
    public String getScheme() {
        return null;
    }

    @Override
    public String getServerName() {
        return request.context().channel().localAddress().toString();
    }

    @Override
    public int getServerPort() {
        return 0;
    }

    @Override
    public BufferedReader getReader()
            throws IOException {
        return null;
    }

    @Override
    public String getRemoteAddr() {
        return request.getRemoteAddress();
    }

    @Override
    public String getRemoteHost() {
        return request.getRemoteAddress();
    }

    @Override
    public void setAttribute(String s, Object o) {
        throw new IllegalStateException();
    }

    @Override
    public void removeAttribute(String s) {
        throw new IllegalStateException();
    }

    @Override
    public Locale getLocale() {
        throw new IllegalStateException();
    }

    @Override
    public Enumeration<Locale> getLocales() {
        throw new IllegalStateException();
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public RequestDispatcher getRequestDispatcher(String s) {
        throw new IllegalStateException();
    }

    @Override
    public String getRealPath(String s) {
        throw new IllegalStateException();
    }

    @Override
    public int getRemotePort() {
        return 0;
    }

    @Override
    public String getLocalName() {
        return null;
    }

    @Override
    public String getLocalAddr() {
        return null;
    }

    @Override
    public int getLocalPort() {
        return 0;
    }

    @Override
    public ServletContext getServletContext() {
        throw new IllegalStateException();
    }

    @Override
    public AsyncContext startAsync()
            throws IllegalStateException {
        throw new IllegalStateException();
    }

    @Override
    public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse)
            throws IllegalStateException {
        throw new IllegalStateException();
    }

    @Override
    public boolean isAsyncStarted() {
        return false;
    }

    @Override
    public boolean isAsyncSupported() {
        return false;
    }

    @Override
    public AsyncContext getAsyncContext() {
        throw new IllegalStateException();
    }

    @Override
    public DispatcherType getDispatcherType() {
        throw new IllegalStateException();
    }
}

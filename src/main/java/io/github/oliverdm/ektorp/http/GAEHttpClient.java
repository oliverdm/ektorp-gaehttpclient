package io.github.oliverdm.ektorp.http;

import com.google.appengine.api.urlfetch.FetchOptions;
import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPMethod;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.IURLFetchServiceFactory;
import com.google.appengine.api.urlfetch.URLFetchService;
import com.google.appengine.api.urlfetch.URLFetchServiceFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.HttpCookie;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.DatatypeConverter;
import org.apache.http.HttpEntity;
import org.ektorp.http.HttpClient;
import org.ektorp.http.HttpResponse;
import org.ektorp.util.Exceptions;

/**
 * {@link HttpClient} implementation that uses Google App Engine's URL Fetch
 * Service to execute requests.
 * The following restrictions apply:
 * <ul>
 *   <li>Maximum request timeout is 60 seconds</li>
 *   <li>Allowed ports are: 80-90, 440-450, 1024-65535<br>
 *       http://... defaults to port 80<br>
 *       https://... defaults to port 443
 *   </li>
 *   <li>Allowed methods are: GET, POST, PUT, HEAD, DELETE</li>
 *   <li>Persistent connection are not supported</li>
 * </ul>
 * @see <a href="https://cloud.google.com/appengine/docs/java/urlfetch/">
 * URL Fetch Java API Overview</a>
 */
public class GAEHttpClient implements HttpClient {
    
    public static final String SESSION_COOKIE = "AuthSession";
    
    private static final Logger logger = Logger.getLogger(GAEHttpClient.class.getName());
    
    private final Client client;
    private final FetchOptions options;
    private final URL baseUrl;
    
    /**
     * Used internally to create a new instance.
     * @param baseUrl base URL for all requests (protocol, host, port)
     * @param client {@link Client} implementation
     * @param options request options for all requests
     */
    public GAEHttpClient(URL baseUrl, Client client, FetchOptions options) {
        if (baseUrl == null) {
            throw new IllegalArgumentException("Base URL must not be null");
        }
        if (client == null) {
            throw new IllegalArgumentException("Client must not be null");
        }
        if (options == null) {
            throw new IllegalArgumentException("Options must not be null");
        }
        this.baseUrl = baseUrl;
        this.client = client;
        this.options = options;
    }
    
    public URL getBaseUrl() {
        return this.baseUrl;
    }
    
    public Client getClient() {
        return this.client;
    }
    
    protected GAEHttpResponse executeRequest(HTTPRequest req) {
        try {
            GAEHttpResponse res = client.execute(req);
            logger.log(Level.INFO, "{0} {1} {2}", new String[]{
                res.getMethod(), 
                res.getRequestURI(),
                res.getCode() + ""
            });
            return res;
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
    protected GAEHttpResponse executeRequest(HTTPRequest req, Map<String, String> headers) {
        if (headers != null) {
            for (Entry<String, String> e : headers.entrySet()) {
                req.addHeader(new HTTPHeader(e.getKey(), e.getValue()));
            }
        }
        return this.executeRequest(req);
    }
    
    protected GAEHttpResponse executePutPost(HTTPRequest req, String content) {
        return this.executePutPost(req, (Map<String, String>) null, content);
    }
    
    protected GAEHttpResponse executePutPost(HTTPRequest req,
            Map<String, String> headers, String content) {
        try {
            return this.executePutPost(req, headers,
                    content != null ? content.getBytes("UTF-8"): null,
                    "application/json");
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
    protected GAEHttpResponse executePutPost(HTTPRequest req, byte[] payload,
            String contentType) {
        return this.executePutPost(req, null, payload, contentType);
    }
    
     protected GAEHttpResponse executePutPost(HTTPRequest req,
             Map<String, String> headers, byte[] payload, String contentType) {
        try {
            if (payload != null) {
                req.setPayload(payload);
                req.setHeader(new HTTPHeader("Content-Type", contentType));
            }
            return this.executeRequest(req, headers);
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
     
    protected GAEHttpResponse executePutPost(HTTPRequest req, HttpEntity entity) {
        if (entity.isChunked()) {
            throw new UnsupportedOperationException("Chunked entity not supported");
        }
        else if (entity.isStreaming()) {
            throw new UnsupportedOperationException("Streaming entity not supported");
        }
        try (InputStream is = entity.getContent()) {
            long contentLength = entity.getContentLength();
            byte[] content = this.write(is, contentLength);
            String contentType = entity.getContentEncoding().getValue();
            return this.executePutPost(req, content, contentType);
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
     /**
      * Creates a new {@link HTTPRequest} instance.
      * @param path the path component of an URL
      * @param method {@link HTTPMethod} enum
      * @return a new {@link HTTPRequest} instance
      */
    protected HTTPRequest req(String path, HTTPMethod method) {
        try {
            URL url = new URL(
                    this.baseUrl.getProtocol(),
                    this.baseUrl.getHost(),
                    this.baseUrl.getPort(),
                    path);
            HTTPRequest req = new HTTPRequest(url, method, this.options);
            return req;
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
    /**
     * Writes the {@link InputStream} content into a <code>byte</code> array.
     * @param is the {@link InputStream}
     * @param contentLength the number of bytes to read from the stream. If less
     * than zero all bytes are read.
     * @return the <code>bytes</code> read from the stream
     */
    protected byte[] write(InputStream is, long contentLength) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] chunk = new byte[2048];
            int read;
            // see org.apache.http.entity.InputStreamEntity#writeTo(OutputStream)
            if (contentLength < 0) {
                while ((read = is.read(chunk)) != -1) {
                    baos.write(chunk, 0, read);
                }
            } else {
                long remaining = contentLength;
                while (remaining > 0) {
                    read = is.read(chunk, 0, (int) Math.min(chunk.length, remaining));
                    if (read == -1) {
                        break;
                    }
                    baos.write(chunk, 0, read);
                    remaining -= read;
                }
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }
    
    // HttpClient methods

    @Override
    public HttpResponse get(String uri) {
        return this.executeRequest(this.req(uri, HTTPMethod.GET));
    }

    @Override
    public HttpResponse get(String uri, Map<String, String> headers) {
        return this.executeRequest(this.req(uri, HTTPMethod.GET), headers);
    }

    @Override
    public HttpResponse put(String uri, String content) {
        return this.executePutPost(this.req(uri, HTTPMethod.PUT), content);
    }

    @Override
    public HttpResponse put(String uri) {
        return this.executePutPost(this.req(uri, HTTPMethod.PUT), (String) null);
    }

    @Override
    public HttpResponse put(String uri, InputStream is, String contentType, long contentLength) {
        byte[] payload = this.write(is, contentLength);
        return this.executePutPost(this.req(uri, HTTPMethod.PUT), payload, contentType);
    }

    @Override
    public HttpResponse post(String uri, String content) {
        return this.executePutPost(this.req(uri, HTTPMethod.POST), content);
    }

    @Override
    public HttpResponse post(String uri, InputStream content) {
        byte[] payload = this.write(content, -1);
        return this.executePutPost(this.req(uri, HTTPMethod.POST), payload, "application/json");
    }

    @Override
    public HttpResponse delete(String uri) {
        return this.executeRequest(this.req(uri, HTTPMethod.DELETE));
    }

    @Override
    public HttpResponse head(String uri) {
        return this.executeRequest(this.req(uri, HTTPMethod.HEAD));
    }

    @Override
    public HttpResponse getUncached(String uri) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Cache-Control", "max-age=0, must-revalidate");
        return this.executeRequest(this.req(uri, HTTPMethod.GET), headers);
    }

    @Override
    public HttpResponse postUncached(String uri, String content) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Cache-Control", "max-age=0, must-revalidate");
        return this.executePutPost(this.req(uri, HTTPMethod.POST), headers, content);
    }

    @Override
    public HttpResponse copy(String sourceUri, String destination) {
        throw new UnsupportedOperationException("Copy is not supported");
    }

    @Override
    public void shutdown() {
    }

    @Override
    public HttpResponse put(String uri, HttpEntity entity) {
        return this.executePutPost(this.req(uri, HTTPMethod.PUT), entity);
    }

    @Override
    public HttpResponse post(String uri, HttpEntity entity) {
        return this.executePutPost(this.req(uri, HTTPMethod.POST), entity);
    }
    
    /**
     * Internally used abstraction for objects that can execute HTTP requests.
     */
    protected static interface Client {
        
        /**
         * Executes the given {@link HTTPRequest}.
         * @param request the request to execute
         * @return a {@link GAEHttpResponse} instance that wraps GAE's
         * {@link com.google.appengine.api.urlfetch.HTTPResponse}.
         * @throws IOException 
         */
        public GAEHttpResponse execute(HTTPRequest request) throws IOException;
        
    }
    
    /**
     * Executes a request without authentication.
     */
    protected static class DefaultClient implements Client {
        
        private final URLFetchService service;
        
        public DefaultClient(URLFetchService service) {
            if (service == null) {
                throw new IllegalArgumentException("URLFetchService must not be null");
            }
            this.service = service;
        }

        @Override
        public GAEHttpResponse execute(HTTPRequest req) throws IOException {
            return new GAEHttpResponse(req.getMethod().name(), req.getURL(),
                    this.service.fetch(req));
        }
    }
    
    /**
     * Executes a request with basic authentication by setting the
     * <code>Authorization: Basic ...</code> header.
     */
    protected static class BasicAuthClient implements Client {

        private final URLFetchService service;
        private final HTTPHeader basicAuthHeader;
        
        /**
         * Constructor with username and password.
         * @param service the {@link URLFetchService}
         * @param username the username, must not be null
         * @param password the raw password, must not be null
         * @throws NullPointerException if username or password is null
         * @throws RuntimeException wrapper for UnsupportedEncodingException
         * when converting to <code>ASCII</code>
         */
        public BasicAuthClient(URLFetchService service, String username, String password)
                throws NullPointerException {
            if (service == null) {
                throw new NullPointerException("URLFetchService must not be null");
            }
            if (username == null) {
                throw new NullPointerException("Username must not be null");
            }
            if (password == null) {
                throw new NullPointerException("Password must not be null");
            }
            this.service = service;
            try {
                this.basicAuthHeader = new HTTPHeader("Authorization",
                        "Basic " + DatatypeConverter.printBase64Binary(
                                (username + ":" + password).getBytes("ASCII")));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public GAEHttpResponse execute(HTTPRequest req) throws IOException {
            req.setHeader(this.basicAuthHeader);
            return new GAEHttpResponse(req.getMethod().name(), req.getURL(),
                    this.service.fetch(req));
        }
        
    }
    
    /**
     * Executes a request with session authentication.
     * Before executing a request a session is started through
     * <code>POST /_session</code>.
     * The session cookie is stored for reuse in subsequent requests.
     * Access to the session cookie is synchronized.
     * An expired session cookie is discarded before the next request.
     * <code>401</code> responses will cause the session cookie to be discarded
     * immediately.
     * Set-Cookie in response headers updates the session cookie.
     */
    protected static class SessionAuthClient implements Client {
        
        private static final Logger logger = 
                Logger.getLogger(SessionAuthClient.class.getName());
        
        private static final HTTPHeader sessionContentType = 
                new HTTPHeader("Content-Type", "application/json");
        
        private final URLFetchService service;
        private final byte[] userPassJsonBytes;
        
        private final Object sessionLock = new Object();
        private volatile HttpCookie session;
        private final long sessionTimeout;
        
        /**
         * Constructor with username and password.
         * @param service the {@link URLFetchService}
         * @param username the username, must not be null
         * @param password the raw password, must not be null
         * @param sessionTimeout the session timeout in seconds, a negative
         * value  means the cookie is kept around until it is rejected by
         * CouchDb
         * @throws NullPointerException if a constructor argument is null
         * @throws RuntimeException wrapper for UnsupportedEncodingException
         * when converting to <code>UTF-8</code>
         */
        public SessionAuthClient(URLFetchService service, String username,
                String password, long sessionTimeout)
                throws NullPointerException, RuntimeException {
            if (service == null) {
                throw new NullPointerException("URLFetchService must not be null");
            }
            if (username == null) {
                throw new NullPointerException("Username must not be null");
            }
            if (password == null) {
                throw new NullPointerException("Password must not be null");
            }
            this.service = service;
            this.sessionTimeout = sessionTimeout;
            try {
                this.userPassJsonBytes = String.format(
                        "{\"name\":\"%s\",\"password\":\"%s\"}",
                        username.replace("\"", "\\\""),
                        password.replace("\"", "\\\""))
                        .getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        
        protected URL sessionUrl(URL url) throws MalformedURLException {
            return new URL(url.getProtocol(), url.getHost(), url.getPort(), "/_session");
        }
        
        protected void setMaxAge(HttpCookie cookie, long timeout) {
            cookie.setMaxAge(timeout > 0 ? timeout : -1);
        }
        
        /**
         * @return a copy of the internally used session cookie as currently
         * seen by this method, can be null
         */
        public HttpCookie getSession() {
            HttpCookie sess = this.session;
            HttpCookie copy = null;
            if (sess != null) {
                synchronized(this.sessionLock) {
                    sess = this.session;
                    copy = new HttpCookie(sess.getName(), sess.getValue());
                    copy.setComment(sess.getComment());
                    copy.setCommentURL(sess.getCommentURL());
                    copy.setDiscard(sess.getDiscard());
                    copy.setDomain(sess.getDomain());
                    copy.setHttpOnly(sess.isHttpOnly());
                    copy.setMaxAge(sess.getMaxAge());
                    copy.setPath(sess.getPath());
                    copy.setPortlist(sess.getPortlist());
                    copy.setSecure(sess.getSecure());
                    copy.setVersion(sess.getVersion());
                }
            }
            return copy;
        }
        
        @Override
        public GAEHttpResponse execute(HTTPRequest request) throws IOException {
            HttpCookie sess = session;
            // start session
            if (sess == null || sess.hasExpired()) {
                synchronized(this.sessionLock) {
                    sess = this.session;
                    if (sess == null || sess.hasExpired()) {
                        HTTPRequest req = new HTTPRequest(
                                this.sessionUrl(request.getURL()),
                                HTTPMethod.POST,
                                request.getFetchOptions());
                        req.setHeader(sessionContentType);
                        req.setPayload(this.userPassJsonBytes);
                        
                        GAEHttpResponse res = new GAEHttpResponse(
                                req.getMethod().name(),
                                req.getURL(),
                                this.service.fetch(req));
                        
                        HttpCookie sessionCookie = res.getCookie(SESSION_COOKIE);
                        if (sessionCookie != null) {
                            this.setMaxAge(sessionCookie, this.sessionTimeout);
                            this.session = sess = sessionCookie;
                            logger.log(Level.INFO, "Session started: {0}", sess);
                        }
                    }
                }
            }
            // set session cookie in request
            if (sess != null) {
                request.addHeader(new HTTPHeader("Cookie", sess.toString()));
            }
            // execute request
            GAEHttpResponse response = new GAEHttpResponse(
                                request.getMethod().name(),
                                request.getURL(),
                                this.service.fetch(request));
            // discard session cookie on 401
            if (401 == response.getCode()) {
                synchronized(this.sessionLock) {
                    this.session = sess = null;
                }
            }
            // renew session from response
            else {
                HttpCookie sessionCookie = response.getCookie(SESSION_COOKIE);
                if (sessionCookie != null) {
                    synchronized(this.sessionLock) {
                        this.setMaxAge(sessionCookie, this.sessionTimeout);
                        this.session = sess = sessionCookie;
                    }
                    logger.log(Level.INFO, "Session regenerated: {0}", sess);
                }
            }
            return response;
        }
        
    }
    
    /**
     * Builds {@link GAEHttpClient} instances.
     */
    public static class Builder {
        
        private final FetchOptions options = FetchOptions.Builder.withDefaults()
                .disallowTruncate()
                .doNotFollowRedirects()
                .validateCertificate();
        private String url = "http://127.0.0.1:5984";
        private String auth;
        private String username;
        private String password;
        private Integer requestTimeout;
        private Long sessionTimeout = 540L;
        private URLFetchService service;
        private Class<?> serviceFactoryClass = URLFetchServiceFactory.class;
        
        public Builder() {}
        
        /**
         * Sets the {@link URLFetchService} to use for all requests.
         * If a non-null instance is provided
         * {@link #withURLFetchServiceFactory} has no effect.
         * By default a new instance is retrieved through the factory class.
         * @param service the service instance
         * @return this builder (for chaining)
         */
        public Builder withURLFetchService(URLFetchService service) {
            this.service = service;
            return this;
        }
        
        /**
         * Sets a {@link URLFetchService} factory class which either
         * <ul>
         * <li>implements {@link IURLFetchServiceFactory}, OR</li>
         * <li>has a static method <code>getURLFetchService()</code> that returns
         * {@link URLFetchService}.</li>
         * </ul>
         * The default is {@link URLFetchServiceFactory}.
         * @param clazz the factory class
         * @return this builder (for chaining)
         */
        public Builder withURLFetchServiceFactory(Class<?> clazz) {
            this.serviceFactoryClass = clazz;
            return this;
        }
        
        /**
         * Sets the base URL for all CouchDb requests.
         * @param url the URL
         * @return this builder (for chaining)
         */
        public Builder url(String url) {
            this.url = url;
            return this;
        }
        
        /**
         * Sets the request timeout (deadline). Defaults to 5 seconds.
         * @param timeout the request timeout in seconds. Cannot be greater than
         * 60 seconds. Negative values will raise an exception later.
         * @return this builder (for chaining)
         */
        public Builder timeout(int timeout) {
            this.requestTimeout = timeout > 60 ? 60 : timeout;
            return this;
        }
        
        /**
         * Enables basic authentication.
         * @param username the username
         * @param password the password
         * @return this builder (for chaining)
         */
        public Builder basicAuth(String username, String password) {
            this.auth = "basic";
            this.username = username;
            this.password = password;
            return this;
        }
        
        /**
         * Enables session authentication with default session timeout of
         * 9 minutes.
         * @param username the username
         * @param password the password
         * @return this builder (for chaining)
         */
        public Builder sessionAuth(String username, String password) {
            return this.sessionAuth(username, password, this.sessionTimeout);
        }
        
        /**
         * Enables session authentication.
         * @param username the username
         * @param password the password
         * @param sessionTimeout the session timeout in seconds, should be
         * slightly less than the session timeout configured in the CouchDb
         * instance
         * @return this builder (for chaining)
         */
        public Builder sessionAuth(String username, String password, long sessionTimeout) {
            this.auth = "session";
            this.username = username;
            this.password = password;
            this.sessionTimeout = sessionTimeout;
            return this;
        }
        
        /**
         * Sets whether to validate SSL certificates.
         * By default certificates are validated.
         * @param relaxed if set to <code>true</code> SSL certificates are not
         * validated
         * @return this builder (for chaining)
         */
        public Builder relaxedSSLSettings(boolean relaxed) {
            if (relaxed) {
                this.options.doNotValidateCertificate();
            } else {
                this.options.validateCertificate();
            }
            return this;
        }
        
        /**
         * Builds a new {@link GAEHttpClient} instance.
         * @return
         * @throws IllegalArgumentException
         * @throws NullPointerException 
         */
        public GAEHttpClient build()
                throws IllegalArgumentException, NullPointerException {
            // create URLFetchService instance
            if (this.service == null) {
                try {
                    if (IURLFetchServiceFactory.class.isAssignableFrom(this.serviceFactoryClass)) {
                        IURLFetchServiceFactory fact = (IURLFetchServiceFactory) 
                                this.serviceFactoryClass.newInstance();
                        service = fact.getURLFetchService();
                    } else {
                        Method m = serviceFactoryClass.getDeclaredMethod("getURLFetchService");
                        service = (URLFetchService) m.invoke(null);
                    }
                } catch(ReflectiveOperationException e) {
                    throw new IllegalArgumentException(e);
                }
            }
            // create base URL
            URL baseUrl;
            try {
                baseUrl = new URL(this.url);
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("Invalid URL: " + this.url);
            }
            // set request deadline
            if (this.requestTimeout != null) {
                this.options.setDeadline((double) this.requestTimeout);
            }
            // create Client instance
            Client client;
            if (null == this.auth) {
                client = new DefaultClient(service);
            }
            else if ("basic".equalsIgnoreCase(this.auth)) {
                client = new BasicAuthClient(service, this.username, this.password);
            }
            else if ("session".equalsIgnoreCase(this.auth)) {
                client = new SessionAuthClient(service, this.username,
                        this.password, this.sessionTimeout);
            }
            else {
                throw new IllegalArgumentException(
                        "Unsupported authentication method (basic, session): "
                                + this.auth);
            }
            // create HttpClient instance
            return new GAEHttpClient(baseUrl, client, this.options);
        }
        
    }

}

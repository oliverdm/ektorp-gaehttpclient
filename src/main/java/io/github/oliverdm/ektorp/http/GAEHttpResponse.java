package io.github.oliverdm.ektorp.http;

import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPResponse;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.HttpCookie;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ektorp.http.HttpResponse;

/**
 * Adapter for {@link HTTPResponse} that implements {@link HttpResponse}.
 */
public class GAEHttpResponse implements HttpResponse {
    
    private static final Logger logger = Logger.getLogger(GAEHttpResponse.class.getName());
    
    private final HTTPResponse response;
    private final String method;
    private final URL requestUrl;
    
    private Map<String, String> headers;
    private List<HttpCookie> cookies;
    
    public GAEHttpResponse(String method, URL requestUrl, HTTPResponse response)
            throws IllegalArgumentException {
        if (method == null) {
            throw new IllegalArgumentException("Method must not be null");
        }
        if (requestUrl == null) {
            throw new IllegalArgumentException("URL must not be null");
        }
        if (response == null) {
            throw new IllegalArgumentException("Response must not be null");
        }
        this.method = method;
        this.requestUrl = requestUrl;
        this.response = response;
    }
    
    public String getMethod() {
        return this.method;
    }
    
    public String getHeader(String name) {
        if (this.headers == null) {
            this.headers = new HashMap<>();
            for (HTTPHeader h : this.response.getHeaders()) {
                this.headers.put(h.getName(), h.getValue());
            }
        }
        return this.headers.get(name);
    }
    
    public List<HttpCookie> getCookies() {
        if (this.cookies == null) {
            try {
                String setCookieStr = this.getHeader("Set-Cookie");
                this.cookies = HttpCookie.parse(setCookieStr);
            } catch (IllegalArgumentException | NullPointerException e) {
                this.cookies = Collections.emptyList();
            }
        }
        return this.cookies;
    }
    
    public HttpCookie getCookie(String name) {
        for (HttpCookie c : this.getCookies()) {
            if (c.getName().equals(name)) {
                return c;
            }
        }
        return null;
    }
    
    // HttpResponse methods
    
    @Override
    public boolean isSuccessful() {
        return this.getCode() < 300;
    }

    @Override
    public int getCode() {
        return this.response.getResponseCode();
    }

    @Override
    public String getRequestURI() {
        return (this.response.getFinalUrl() != null ?
                this.response.getFinalUrl() : this.requestUrl).toString();
    }

    @Override
    public String getContentType() {
        return this.getHeader("Content-Type");
    }

    @Override
    public long getContentLength() {
        try {
            String strLen = this.getHeader("Content-Length");
            if (strLen != null) {
                return Long.parseLong(strLen);
            }
        } catch (NumberFormatException e) {
            logger.log(Level.WARNING, e.getMessage());
        }
        return 0;
    }

    @Override
    public InputStream getContent() {
        return new ByteArrayInputStream(this.response.getContent());
    }

    @Override
    public String getETag() {
        return this.getHeader("ETag");
    }

    @Override
    public void releaseConnection() {
    }

    @Override
    public void abort() {
    }
    
    @Override
    public String toString() {
        return "" + this.getCode();
    }

}

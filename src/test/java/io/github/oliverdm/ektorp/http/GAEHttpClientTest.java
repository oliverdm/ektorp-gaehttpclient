package io.github.oliverdm.ektorp.http;

import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPMethod;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.HTTPResponse;
import com.google.appengine.api.urlfetch.URLFetchService;
import java.io.IOException;
import java.net.HttpCookie;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.github.oliverdm.ektorp.http.GAEHttpClient.SessionAuthClient;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.impl.StdCouchDbInstance;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GAEHttpClientTest {
    
    private static final Logger logger = Logger.getLogger(GAEHttpClientTest.class.getName());
    
    private static final String dbName = "fakedb";
    
    private MockURLFetchService fetchService;
    
    @Before
    public void setUp() {
        fetchService = new MockURLFetchService();
    }
    
    @Test
    public void testSessionTimeout() throws Exception {
        final GAEHttpClient client = new GAEHttpClient.Builder()
                .url("http://127.0.0.1:5984")
                .sessionAuth("fakeadmin", "fakepass", 2) // 2 second session timeout
                .withURLFetchService(this.fetchService)
                .build();
        
        SessionAuthClient authClient = (SessionAuthClient) client.getClient();
        assertNull(authClient.getSession());
        
        // connect to CouchDb: session cookie must be available
        CouchDbInstance dbInstance = new StdCouchDbInstance(client);
        dbInstance.createConnector(dbName, true);
        HttpCookie initialSession = authClient.getSession();
        assertNotNull(initialSession);
        
        // wait for a second
        Thread.sleep(1000);
        
        // connect again: session should be the same
        dbInstance = new StdCouchDbInstance(client);
        dbInstance.createConnector(dbName, true);
        assertEquals(initialSession.getValue(), authClient.getSession().getValue());
        
        // session should be expired by now...
        Thread.sleep(2000);
        
        // connect a third time: a new session should have been started
        dbInstance = new StdCouchDbInstance(client);
        dbInstance.createConnector(dbName, true);
        assertFalse(initialSession.getValue().equals(authClient.getSession().getValue()));
        assertEquals(2, this.fetchService.sessions.size());
    }
    
    @Test
    public void testConcurrentSessionAuth() throws Exception {
        final GAEHttpClient client = new GAEHttpClient.Builder()
                .url("http://127.0.0.1:5984")
                .sessionAuth("fakeadmin", "fakepass")
                .relaxedSSLSettings(true)
                .withURLFetchService(this.fetchService)
                .build();
        
        // access database concurrently
        int threadCount = 10;
        List<Thread> threads = new ArrayList<>(threadCount);
        final boolean[] errors = new boolean[threadCount];
        for (int i=0; i < threadCount; i++) {
            final int index = i;
            Thread t = new Thread() {
                @Override public void run() {
                    logger.log(Level.INFO, "START #{0} ", index + "");
                    CouchDbConnector db;
                    try {
                        CouchDbInstance dbInstance = new StdCouchDbInstance(client);
                        db = dbInstance.createConnector(dbName, true);
                    } catch (Exception e) {
                        db = null;
                    }
                    errors[index] = db == null;
                    logger.log(Level.INFO, "DONE #{0}", index + "");
                }
            };
            t.start();
            threads.add(t);
        }
        // wait for threads to complete
        for (Thread t : threads) {
            t.join();
        }
        // check no errors in threads
        assertArrayEquals(new boolean[threadCount], errors);
        // check only one session was used
        assertEquals(1, this.fetchService.sessions.size());
    }
    
    // URLFetchService mock that removes the need for CouchDB instance and 
    // LocalURLFetchServiceTestConfig (which does not work well with threads)
    public static class MockURLFetchService implements URLFetchService {
        
        public final List<String> sessions = new ArrayList<>();
        
        @Override public HTTPResponse fetch(HTTPRequest req) throws IOException {
            // build a mock response
            HTTPResponse res = mock(HTTPResponse.class);
            when(res.getFinalUrl()).thenReturn(req.getURL());
            List<HTTPHeader> headers = new ArrayList<>();
            headers.add(new HTTPHeader("Server", "CouchDB/1.6.1 (Erlang OTP/R16B02)"));
            headers.add(new HTTPHeader("Date", "Tue, 06 Jan 2015 13:09:40 GMT"));
            headers.add(new HTTPHeader("Content-Type", "text/plain; charset=utf-8"));
            headers.add(new HTTPHeader("Cache-Control", "must-revalidate"));

            // POST /_session
            if (req.getURL().getPath().startsWith("/_session")
                    && HTTPMethod.POST.equals(req.getMethod())) {
                String sessionId = UUID.randomUUID().toString();
                synchronized(this.sessions) {
                    sessions.add(sessionId);
                }
                when(res.getResponseCode()).thenReturn(200);
                headers.add(new HTTPHeader("Set-Cookie",
                        "AuthSession=" + sessionId + "; Version=1; Path=/; HttpOnly"));
                headers.add(new HTTPHeader("Content-Length", "43"));
                when(res.getContent()).thenReturn(
                        "{\"ok\":true,\"name\":null,\"roles\":[\"_admin\"]}"
                                .getBytes("UTF-8"));
            }
            // HEAD /<db-name>
            else if (req.getURL().getPath().startsWith("/" + dbName)
                    && HTTPMethod.HEAD.equals(req.getMethod())) {
                when(res.getResponseCode()).thenReturn(404);
                headers.add(new HTTPHeader("Content-Length", "44"));
                when(res.getContent()).thenReturn(new byte[0]);
            }
            // PUT /<db-name>
            else if (req.getURL().getPath().startsWith("/" + dbName)
                    && HTTPMethod.PUT.equals(req.getMethod())) {
                when(res.getResponseCode()).thenReturn(201);
                headers.add(new HTTPHeader("Content-Length", "12"));
                headers.add(new HTTPHeader("Location", new URL(
                        req.getURL().getProtocol(),
                        req.getURL().getHost(),
                        "/" + dbName
                ).toString()));
                when(res.getContent()).thenReturn("{\"ok\":true}".getBytes("UTF-8"));
            }

            when(res.getHeaders()).thenReturn(headers);
            return res;
        }
        // these methods are known to be unused...
        @Override public HTTPResponse fetch(URL url) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override public Future<HTTPResponse> fetchAsync(URL url) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override public Future<HTTPResponse> fetchAsync(HTTPRequest httpr) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    };

}

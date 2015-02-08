# Ektorp GAEHttpClient

Experimental!

Implementation of `org.ektorp.http.HttpClient` and `org.ektorp.http.HttpResponse` using [Google AppEngine URLFetchService](https://cloud.google.com/appengine/docs/java/javadoc/com/google/appengine/api/urlfetch/URLFetchService) to interact with [CouchDB](http://couchdb.apache.org/) instances from within GAE.
Ektorp's default implementation (based on Apache `org.apache.http.client.HttpClient`) [does not work on Google AppEngine](https://groups.google.com/forum/#!topic/ektorp-discuss/uZDpkUE116s).
Besides implementing `HttpClient` there is also the option to make Apache `HttpClient` [compatible with GAE](https://github.com/LeviticusMB/esxx/tree/master/jee/org/esxx/js/protocol/).

### Caveats
 
 * Experimental!
 * GAE limits requests to 60 seconds and does not allow persistent connections
 * `COPY` not supported

### Features

 * Supports [Ektorp](http://ektorp.org/) version `1.4.0` and `1.4.3` (see branches)
 * Basic and session authentication at this point

### Usage

`GAEHttpClient.Builder` with all options (more documentation in source code):

```
HttpClient httpClient = new GAEHttpClient.Builder()
        .url("http://127.0.0.1:5984")             // CouchDB URL
        .timeout(5)                               // request timeout in seconds <= 60
        .basicAuth("username", "password")        // basic authentication
        .sessionAuth("username", "password")      // session authentication, session shared across requests
        .sessionAuth("username", "password", 540) // session authentication with custom session timeout in seconds
        .relaxedSSLSettings(true)                 // do not validate certificate (e.g. during development)
        .build();
CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
```

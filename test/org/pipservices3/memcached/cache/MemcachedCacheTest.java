package org.pipservices3.memcached.cache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.memcached.fixtures.CacheFixture;

import java.io.IOException;

public class MemcachedCacheTest {
    MemcachedCache _cache;
    CacheFixture _fixture;

    @Before
    public void setup() throws ApplicationException {
        var host = System.getenv("MEMCACHED_SERVICE_HOST") != null ? System.getenv("MEMCACHED_SERVICE_HOST") : "localhost";
        var port = System.getenv("MEMCACHED_SERVICE_PORT") != null ? Integer.parseInt(System.getenv("MEMCACHED_SERVICE_PORT")) : 11211;

        _cache = new MemcachedCache();

        var config = ConfigParams.fromTuples(
                "connection.host", host,
                "connection.port", port
        );
        _cache.configure(config);

        _fixture = new CacheFixture(_cache);

        _cache.open(null);
    }

    @After
    public void teardown() {
        _cache.close(null);
    }

    @Test
    public void testStoreAndRetrieve() throws InterruptedException, IOException {
        _fixture.testStoreAndRetrieve();
    }

    @Test
    public void testRetrieveExpired() throws InterruptedException {
        _fixture.testRetrieveExpired();
    }

    @Test
    public void testRemove() {
        _fixture.testRemove();
    }
}

package org.pipservices3.memcached.lock;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.memcached.fixtures.LockFixture;

public class RedisLockTest {
    MemcachedLock _lock;
    LockFixture _fixture;

    @Before
    public void setup() throws ApplicationException {
        var host = System.getenv("MEMCACHED_SERVICE_HOST") != null ? System.getenv("MEMCACHED_SERVICE_HOST") : "localhost";
        var port = System.getenv("MEMCACHED_SERVICE_PORT") != null ? Integer.parseInt(System.getenv("MEMCACHED_SERVICE_PORT")) : 11211;

        _lock = new MemcachedLock();

        var config = ConfigParams.fromTuples(
                "connection.host", host,
                "connection.port", port
        );
        _lock.configure(config);

        _fixture = new LockFixture(_lock);

        _lock.open(null);
    }

    @After
    public void teardown() {
        _lock.close(null);
    }

    @Test
    public void testTryAcquireLock() {
        _fixture.testTryAcquireLock();
    }

    @Test
    public void testAcquireLock() throws ApplicationException, InterruptedException {
        _fixture.testAcquireLock();
    }

    @Test
    public void testReleaseLock() {
        _fixture.testReleaseLock();
    }
}

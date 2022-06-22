package org.pipservices3.memcached.build;

import org.pipservices3.commons.refer.Descriptor;
import org.pipservices3.components.build.Factory;
import org.pipservices3.memcached.cache.MemcachedCache;
import org.pipservices3.memcached.lock.MemcachedLock;

/**
 * Creates Redis components by their descriptors.
 *
 * @see MemcachedCache
 * @see MemcachedLock
 */
public class DefaultMemcachedFactory extends Factory {
    private static final Descriptor MemcachedCacheDescriptor = new Descriptor("pip-services", "cache", "memcached", "*", "1.0");
    private static final Descriptor MemcachedLockDescriptor = new Descriptor("pip-services", "lock", "memcached", "*", "1.0");

    /**
     * Create a new instance of the factory.
     */
    public DefaultMemcachedFactory() {
        super();
        this.registerAsType(DefaultMemcachedFactory.MemcachedCacheDescriptor, MemcachedCache.class);
        this.registerAsType(DefaultMemcachedFactory.MemcachedLockDescriptor, MemcachedLock.class);
    }
}

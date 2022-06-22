package org.pipservices3.memcached.lock;

import net.rubyeye.xmemcached.XMemcachedClient;
import net.rubyeye.xmemcached.exception.MemcachedException;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.config.IConfigurable;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.commons.errors.ConfigException;
import org.pipservices3.commons.errors.InvalidStateException;
import org.pipservices3.commons.refer.IReferenceable;
import org.pipservices3.commons.refer.IReferences;
import org.pipservices3.commons.run.IOpenable;
import org.pipservices3.components.connect.ConnectionResolver;
import org.pipservices3.components.lock.Lock;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class MemcachedLock extends Lock implements IConfigurable, IReferenceable, IOpenable {
    private final ConnectionResolver _connectionResolver = new ConnectionResolver();

//    private int _maxKeySize = 250;
//    private int _maxExpiration = 2592000;
//    private int _maxValue = 1048576;
//    private int _poolSize = 5;
//    private int _reconnect = 10000;
//    private int _timeout = 5000;
//    private int _retries = 5;
//    private int _failures = 5;
//    private int _retry = 30000;
//    private boolean _remove = false;
//    private int _idle = 5000;

    private XMemcachedClient _client = null;

    @Override
    public void configure(ConfigParams config) {
        super.configure(config);

        this._connectionResolver.configure(config);

//        todo this options is not supported
//        this._maxKeySize = config.getAsIntegerWithDefault("options.max_key_size", this._maxKeySize);
//        this._maxExpiration = config.getAsLongWithDefault("options.max_expiration", this._maxExpiration);
//        this._maxValue = config.getAsLongWithDefault("options.max_value", this._maxValue);
//        this._poolSize = config.getAsIntegerWithDefault("options.pool_size", this._poolSize);
//        this._reconnect = config.getAsIntegerWithDefault("options.reconnect", this._reconnect);
//        this._timeout = config.getAsIntegerWithDefault("options.timeout", this._timeout);
//        this._retries = config.getAsIntegerWithDefault("options.retries", this._retries);
//        this._failures = config.getAsIntegerWithDefault("options.failures", this._failures);
//        this._retry = config.getAsIntegerWithDefault("options.retry", this._retry);
//        this._remove = config.getAsBooleanWithDefault("options.remove", this._remove);
//        this._idle = config.getAsIntegerWithDefault("options.idle", this._idle);
    }

    /**
     * Sets references to dependent components.
     *
     * @param references references to locate the component dependencies.
     */
    @Override
    public void setReferences(IReferences references) {
        this._connectionResolver.setReferences(references);
    }

    /**
     * Checks if the component is opened.
     *
     * @return true if the component has been opened and false otherwise.
     */
    @Override
    public boolean isOpen() {
        return this._client != null;
    }

    /**
     * Opens the component.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     */
    @Override
    public void open(String correlationId) throws ApplicationException {
        var connections = this._connectionResolver.resolveAll(correlationId);
        if (connections.size() == 0) {
            throw new ConfigException(
                    correlationId,
                    "NO_CONNECTION",
                    "Connection is not configured"
            );
        }

        try {
            _client = new XMemcachedClient();

            for (var connection : connections) {
                var host = connection.getHost();
                var port = connection.getAsIntegerWithDefault("port", 11211);

                _client.addServer(host, port);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Closes component and frees used resources.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     */
    @Override
    public void close(String correlationId) {
        try {
            _client.shutdown();
            _client = null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkOpened(String correlationId) {
        if (!this.isOpen()) {
            throw new RuntimeException(
                    new InvalidStateException(
                            correlationId,
                            "NOT_OPENED",
                            "Connection is not opened"
                    )
            );
        }
    }

    /**
     * Makes a single attempt to acquire a lock by its key.
     * It returns immediately a positive or negative result.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param key           a unique lock key to acquire.
     * @param ttl           a lock timeout (time to live) in milliseconds.
     * @return <code>true</code> if lock was successfull and <code>false</code> otherwise.
     */
    @Override
    public boolean tryAcquireLock(String correlationId, String key, int ttl) {
        this.checkOpened(correlationId);

        var lifetimeInSec = ttl / 1000;

        try {
            return this._client.add(key, lifetimeInSec, "lock");
        } catch (TimeoutException | InterruptedException | MemcachedException e) {
            if (e.getMessage().contains("not stored"))
                return false;
            throw new RuntimeException(e);
        }
    }

    /**
     * Releases previously acquired lock by its key.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param key           a unique lock key to release.
     */
    @Override
    public void releaseLock(String correlationId, String key) {
        this.checkOpened(correlationId);

        try {
            _client.delete(key);
        } catch (TimeoutException | InterruptedException | MemcachedException e) {
            throw new RuntimeException(e);
        }
    }
}

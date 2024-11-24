package hexsook.datago;

import com.google.common.net.HostAndPort;
import hexsook.originext.Threads;
import hexsook.originext.object.Strings;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisException;

import java.util.UUID;

public class Redis {

    private final String address;
    private final String password;
    private final int timeout;

    private JedisPool connectionPool;

    public Redis(String address, String password, int timeout) {
        this.address = address;
        this.password = password;
        this.timeout = timeout;
    }

    public JedisPool getConnectionPool() {
        return connectionPool;
    }

    public void connect() {
        JedisPoolConfig config = new JedisPoolConfig() {{
            setMaxTotal(100);
            setMaxIdle(5);
            setMinIdle(1);
        }};
        HostAndPort address = HostAndPort.fromString(this.address);
        String host = address.getHost();
        int port = address.getPortOrDefault(6379);

        if (Strings.isNullOrWhite(password)) {
            connectionPool = new JedisPool(config, host, port, timeout);
        } else {
            connectionPool = new JedisPool(config, host, port, timeout, password);
        }
    }

    public boolean isConnected() {
        if (connectionPool == null) {
            return false;
        }

        return "PONG".equals(get().ping());
    }

    public void disconnect() {
        connectionPool.destroy();
        connectionPool = null;
    }

    public Jedis get() {
        return connectionPool.getResource();
    }

    public void listen(String channel, JedisPubSub listener) throws JedisException {
        Threads.runInstant(() -> {
            try (Jedis jedis = connectionPool.getResource()) {
                jedis.subscribe(listener, channel);
            }
        });
    }

    public String publish(RedisMessage message) throws JedisException {
        String messageId = UUID.randomUUID().toString();
        RedisMessage fixedMessage = RedisMessage.builder(message).append("OUTGOING_MESSAGE_ID", messageId).build();

        Threads.runInstant(() -> {
            try (Jedis jedis = connectionPool.getResource()) {
                jedis.publish(fixedMessage.getChannel(), fixedMessage.toString());
            }
        });
        return messageId;
    }

    public String publish(RedisMessage message, RedisCallback callback, long timeout) throws JedisException {
        String messageId = publish(message);
        RedisMessage fixedMessage = RedisMessage.builder(message).append("OUTGOING_MESSAGE_ID", messageId).build();
        RedisCallbackProcessor processor = new RedisCallbackProcessor(fixedMessage.getChannel(), fixedMessage, callback, timeout);
        Threads.runInstant(() -> {
            try {
                listen("callback_queue", processor);
            } catch (JedisException e) {
                callback.error(e);
                processor.stop();
            }
        });
        return messageId;
    }

    public String reply(RedisMessage message, String target) throws JedisException {
        return publish(RedisMessage.builder(message)
                .append("CALLBACK_REPLY_TARGET_ID", target)
                .channel("callback_queue")
                .build());
    }
}
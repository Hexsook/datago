package hexsook.datago;

public interface RedisCallback {

    void success(RedisMessage message);

    void error(Exception e);

    void timeout();
}
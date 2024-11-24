package hexsook.datago;

import hexsook.originext.Threads;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.TimeUnit;

public class RedisCallbackProcessor extends JedisPubSub {

    private final String source;
    private final RedisMessage incoming;
    private final RedisCallback callback;
    private final long timeout;

    private boolean responded;

    public RedisCallbackProcessor(String source, RedisMessage incoming, RedisCallback callback, long timeout) {
        this.source = source;
        this.incoming = incoming;
        this.callback = callback;
        this.timeout = timeout;
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
        Threads.runLater(() -> {
            if (!responded) {
                callback.timeout();
            }
            stop();
        }, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onMessage(String channel, String message) {
        if (responded) {
            stop();
            return;
        }

        try {
            RedisMessage msg = new RedisMessage(message);
            if (!msg.getAction().equalsIgnoreCase(incoming.getAction())) {
                return;
            }

            String targetId = (String) msg.getOption("CALLBACK_REPLY_TARGET_ID");

            if (!incoming.getMessageId().equals(targetId)) {
                return;
            }
            callback.success(msg);

            responded = true;
            stop();
        } catch (Exception e) {
            callback.error(e);
        }
    }

    public void stop() {
        if (isSubscribed()) {
            unsubscribe();
        }
    }

    public String getSource() {
        return source;
    }

    public boolean isResponded() {
        return responded;
    }
}
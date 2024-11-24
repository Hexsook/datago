package hexsook.datago;

import com.google.gson.Gson;
import hexsook.originext.format.Formatter;
import hexsook.originext.format.presets.NumberedPlaceholderFormat;

import java.util.HashMap;
import java.util.Map;

public class RedisMessage {

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(RedisMessage message) {
        return builder()
                .channel(message.getChannel())
                .action(message.getAction())
                .importAll(message.getOptions());
    }

    private static final Gson gson = new Gson();
    private final String channel;
    private final String action;
    private final Map<String, Object> options;

    public RedisMessage(String channel, String action, Map<String, Object> options) {
        this.channel = channel;
        this.action = action;
        this.options = options;
    }

    @SuppressWarnings("unchecked")
    public RedisMessage(String message) {
        Map<String, Object> json = gson.fromJson(message, Map.class);
        validateFormat(json);
        this.channel = (String) json.get("channel");
        this.action = (String) json.get("action");
        this.options = (Map<String, Object>) json.get("options");
    }

    private void validateFormat(Map<String, Object> message) {
        if (message.containsKey("channel") && message.containsKey("action") && message.containsKey("options")) return;
        throw new IllegalArgumentException("illegal message format");
    }

    public boolean hasOption(String key) {
        return options.containsKey(key);
    }

    public Object getOption(String key) {
        return options.get(key);
    }

    public String getMessageId() {
        return (String) getOption("OUTGOING_MESSAGE_ID");
    }

    public String getChannel() {
        return channel;
    }

    public String getAction() {
        return action;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    @Override
    public String toString() {
        return Formatter.format("{\"channel\":\"{0}\", \"action\":\"{1}\", \"options\":{2}}",
                NumberedPlaceholderFormat.create(channel, action, gson.toJson(options)));
    }

    public static class Builder {

        private String channel;
        private String action;
        private final Map<String, Object> options = new HashMap<>();

        public Builder action(String action) {
            this.action = action;
            return this;
        }


        public Builder channel(String channel) {
            this.channel = channel;
            return this;
        }

        public Builder append(String key, Object value) {
            options.put(key, value);
            return this;
        }

        public Builder delete(String key) {
            options.remove(key);
            return this;
        }

        public Builder importAll(Map<String, Object> map) {
            options.putAll(map);
            return this;
        }

        public RedisMessage build() {
            if (channel == null || action == null) {
                throw new IllegalArgumentException("missing channel or action");
            }
            return new RedisMessage(channel, action, options);
        }
    }
}
package firs_try;

import java.util.HashMap;
import java.util.Map;

public class BrokerRegistry {
    private static final Map<String, QueueBroker> brokers = new HashMap<>();

    public static synchronized void register(QueueBroker b) {
        brokers.put(b.name(), b);
    }

    public static synchronized QueueBroker lookup(String name) {
        return brokers.get(name);
    }
}
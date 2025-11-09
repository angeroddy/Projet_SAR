package implementation;

import java.util.HashMap;
import java.util.Map;
import abstracts.Broker;

/**
 * Global registry for broker lookup by name.
 */
public class BrokerManager {
    private static BrokerManager instance = null;
    private Map<String, Broker> brokers = new HashMap<>();

    private BrokerManager() {
    }

    public static BrokerManager getInstance() {
        if (instance == null) {
            instance = new BrokerManager();
        }
        return instance;
    }

    public void addBroker(String name, Broker broker) {
        brokers.put(name, broker);
    }

    public Broker getBroker(String name) {
        return brokers.get(name);
    }
}
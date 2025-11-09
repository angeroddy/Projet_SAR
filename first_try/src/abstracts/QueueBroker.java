package abstracts;

/**
 * Wraps a broker to work with message queues instead of raw channels.
 */
public abstract class QueueBroker {
    protected Broker broker;

    public QueueBroker(Broker broker) {
        this.broker = broker;
    }

    public Broker getBroker() {
        return broker;
    }

    public String name() {
        return broker.name;
    }

    public abstract MessageQueue accept(int port);
    public abstract MessageQueue connect(String name, int port);
}

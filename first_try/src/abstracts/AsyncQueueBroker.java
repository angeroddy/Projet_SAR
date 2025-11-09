package abstracts;

/**
 * Async version of QueueBroker using callbacks instead of blocking.
 */
public abstract class AsyncQueueBroker {
    protected String name;

    public AsyncQueueBroker(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    public interface AcceptListener {
        void accepted(AsyncMessageQueue queue);
    }

    public abstract boolean bind(int port, AcceptListener listener);
    public abstract boolean unbind(int port);

    public interface ConnectListener {
        void connected(AsyncMessageQueue queue);
        void refused();
    }

    public abstract boolean connect(String name, int port, ConnectListener listener);
}

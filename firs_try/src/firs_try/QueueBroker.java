package firs_try;

public abstract class QueueBroker {
    protected String name;

    public QueueBroker(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    public abstract MessageQueue accept(int port);
    public abstract MessageQueue connect(String name, int port);
}


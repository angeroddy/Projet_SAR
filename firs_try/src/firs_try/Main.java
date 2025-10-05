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

public abstract class MessageQueue {
    public abstract void send(byte[] bytes, int offset, int length);
    public abstract byte[] receive();
    public abstract void close();
    public abstract boolean closed();
}

public class Main {

}

package abstracts;

/**
 * Async message queue - uses listener callbacks instead of blocking receive.
 */
public abstract class AsyncMessageQueue {

    public interface Listener {
        void received(byte[] msg);
        void closed();
    }

    public abstract void setListener(Listener l);
    public abstract boolean send(byte[] bytes);
    public abstract boolean send(byte[] bytes, int offset, int length);
    public abstract void close();
    public abstract boolean closed();
}

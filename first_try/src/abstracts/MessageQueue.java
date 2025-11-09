package abstracts;

/**
 * Message-oriented abstraction over raw byte channels.
 * Uses simple length-prefixed protocol (4 bytes big-endian + data).
 */
public abstract class MessageQueue {

    public abstract void send(byte[] bytes, int offset, int length);
    public abstract byte[] receive();
    public abstract void close();
    public abstract boolean closed();
}

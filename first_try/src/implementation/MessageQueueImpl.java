package implementation;

import abstracts.Channel;
import abstracts.MessageQueue;
import java.nio.ByteBuffer;

/**
 * Message queue wrapping a raw byte channel.
 * Uses simple 4-byte length prefix protocol.
 */
public class MessageQueueImpl extends MessageQueue {
    private Channel channel;
    private boolean closed = false;

    public MessageQueueImpl(Channel channel) {
        this.channel = channel;
    }

    @Override
    public void send(byte[] bytes, int offset, int length) {
        if (closed || channel.disconnected()) {
            return;
        }

        // Send length header (4 bytes big-endian)
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        lengthBuffer.putInt(length);
        byte[] lengthBytes = lengthBuffer.array();

        int totalSent = 0;
        while (totalSent < 4) {
            int sent = channel.write(lengthBytes, totalSent, 4 - totalSent);
            if (sent == 0) {
                return;
            }
            totalSent += sent;
        }

        // Send message body
        totalSent = 0;
        while (totalSent < length) {
            int sent = channel.write(bytes, offset + totalSent, length - totalSent);
            if (sent == 0) {
                return;
            }
            totalSent += sent;
        }
    }

    @Override
    public byte[] receive() {
        if (closed && channel.disconnected()) {
            return null;
        }

        // Read length header
        byte[] lengthBytes = new byte[4];
        int totalRead = 0;
        while (totalRead < 4) {
            int read = channel.read(lengthBytes, totalRead, 4 - totalRead);
            if (read == 0) {
                return null;
            }
            totalRead += read;
        }

        ByteBuffer lengthBuffer = ByteBuffer.wrap(lengthBytes);
        int messageLength = lengthBuffer.getInt();

        if (messageLength < 0 || messageLength > 1024 * 1024) {
            throw new IllegalStateException("Invalid message length: " + messageLength);
        }

        // Read message body
        byte[] message = new byte[messageLength];
        totalRead = 0;
        while (totalRead < messageLength) {
            int read = channel.read(message, totalRead, messageLength - totalRead);
            if (read == 0) {
                throw new IllegalStateException("Unexpected end of stream");
            }
            totalRead += read;
        }

        return message;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            channel.disconnect();
        }
    }

    @Override
    public boolean closed() {
        return closed || channel.disconnected();
    }

    public Channel getChannel() {
        return channel;
    }
}

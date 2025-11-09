package implementation;

import abstracts.Channel;
import utils.CircularBuffer;

/**
 * Channel implementation using crossed circular buffers for full-duplex communication.
 * Each channel has an in-buffer (reads) and out-buffer (writes).
 */
public class ChannelImpl extends Channel {
    private CircularBuffer inBuffer;
    private CircularBuffer outBuffer;
    private boolean disconnected = false;

    public ChannelImpl(CircularBuffer inBuffer, CircularBuffer outBuffer) {
        this.inBuffer = inBuffer;
        this.outBuffer = outBuffer;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
        if (disconnected && inBuffer.closed() && inBuffer.empty()) {
            return 0;
        }
        return inBuffer.pull(bytes, offset, length);
    }

    @Override
    public int write(byte[] bytes, int offset, int length) {
        if (disconnected) {
            return 0;
        }
        return outBuffer.push(bytes, offset, length);
    }

    @Override
    public void disconnect() {
        if (!disconnected) {
            disconnected = true;
            outBuffer.close();
        }
    }

    @Override
    public boolean disconnected() {
        return disconnected;
    }

    // for testing
    public CircularBuffer getInBuffer() {
        return inBuffer;
    }

    public CircularBuffer getOutBuffer() {
        return outBuffer;
    }
}

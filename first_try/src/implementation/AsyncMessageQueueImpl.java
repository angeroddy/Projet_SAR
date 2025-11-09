package implementation;

import abstracts.AsyncMessageQueue;
import abstracts.EventPump;
import abstracts.MessageQueue;

/**
 * Async message queue wrapping a sync queue.
 * Uses background thread for reception + event pump for callbacks.
 */
public class AsyncMessageQueueImpl extends AsyncMessageQueue {
    private MessageQueue syncQueue;
    private EventPump eventPump;
    private Listener listener;
    private Thread receptionThread;
    private volatile boolean closed = false;

    public AsyncMessageQueueImpl(MessageQueue syncQueue, EventPump eventPump) {
        this.syncQueue = syncQueue;
        this.eventPump = eventPump;
        startReceptionThread();
    }

    private void startReceptionThread() {
        receptionThread = new Thread(() -> {
            while (!closed && !syncQueue.closed()) {
                try {
                    byte[] msg = syncQueue.receive();
                    if (msg != null && msg.length > 0 && listener != null) {
                        eventPump.post(() -> {
                            if (listener != null) {
                                listener.received(msg);
                            }
                        });
                    } else if (syncQueue.closed()) {
                        break;
                    }
                } catch (Exception e) {
                    break;
                }
            }

            // Notify close
            if (listener != null && !closed) {
                eventPump.post(() -> {
                    if (listener != null) {
                        listener.closed();
                    }
                });
            }
        }, "AsyncMQ-Reception");

        receptionThread.setDaemon(true);
        receptionThread.start();
    }

    @Override
    public void setListener(Listener l) {
        this.listener = l;
    }

    @Override
    public boolean send(byte[] bytes) {
        return send(bytes, 0, bytes.length);
    }

    @Override
    public boolean send(byte[] bytes, int offset, int length) {
        if (closed || syncQueue.closed()) {
            return false;
        }

        // TODO: probably should use a thread pool here
        new Thread(() -> {
            try {
                syncQueue.send(bytes, offset, length);
            } catch (Exception e) {
                // send failed
            }
        }, "AsyncMQ-Send").start();

        return true;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            syncQueue.close();

            if (listener != null) {
                eventPump.post(() -> {
                    if (listener != null) {
                        listener.closed();
                    }
                });
            }
        }
    }

    @Override
    public boolean closed() {
        return closed || syncQueue.closed();
    }
}

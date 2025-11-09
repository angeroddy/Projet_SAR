package implementation;

import abstracts.AsyncMessageQueue;
import abstracts.AsyncQueueBroker;
import abstracts.EventPump;
import abstracts.MessageQueue;
import abstracts.QueueBroker;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Async broker wrapper using event pump for callbacks.
 */
public class AsyncQueueBrokerImpl extends AsyncQueueBroker {
    private QueueBroker syncBroker;
    private EventPump eventPump;
    private ConcurrentHashMap<Integer, AcceptorThread> acceptors;

    public AsyncQueueBrokerImpl(String name, QueueBroker syncBroker, EventPump eventPump) {
        super(name);
        this.syncBroker = syncBroker;
        this.eventPump = eventPump;
        this.acceptors = new ConcurrentHashMap<>();
    }

    @Override
    public boolean bind(int port, AcceptListener listener) {
        if (acceptors.containsKey(port)) {
            return false;
        }

        AcceptorThread acceptor = new AcceptorThread(port, listener);
        acceptors.put(port, acceptor);
        acceptor.start();
        return true;
    }

    @Override
    public boolean unbind(int port) {
        AcceptorThread acceptor = acceptors.remove(port);
        if (acceptor != null) {
            acceptor.shutdown();
            return true;
        }
        return false;
    }

    @Override
    public boolean connect(String name, int port, ConnectListener listener) {
        new Thread(() -> {
            try {
                MessageQueue syncQueue = syncBroker.connect(name, port);
                if (syncQueue != null) {
                    AsyncMessageQueue asyncQueue = new AsyncMessageQueueImpl(syncQueue, eventPump);
                    eventPump.post(() -> listener.connected(asyncQueue));
                } else {
                    eventPump.post(() -> listener.refused());
                }
            } catch (Exception e) {
                eventPump.post(() -> listener.refused());
            }
        }, "AsyncBroker-Connect").start();

        return true;
    }

    // Background acceptor for incoming connections
    private class AcceptorThread extends Thread {
        private int port;
        private AcceptListener listener;
        private AtomicBoolean running;

        public AcceptorThread(int port, AcceptListener listener) {
            super("AsyncBroker-Accept-" + port);
            this.port = port;
            this.listener = listener;
            this.running = new AtomicBoolean(true);
            setDaemon(true);
        }

        @Override
        public void run() {
            while (running.get()) {
                try {
                    MessageQueue syncQueue = syncBroker.accept(port);
                    if (syncQueue != null && running.get()) {
                        AsyncMessageQueue asyncQueue = new AsyncMessageQueueImpl(syncQueue, eventPump);
                        eventPump.post(() -> listener.accepted(asyncQueue));
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ie) {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        public void shutdown() {
            running.set(false);
            this.interrupt();
        }
    }
}

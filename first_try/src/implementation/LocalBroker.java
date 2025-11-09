package implementation;

import abstracts.Broker;
import abstracts.Channel;
import utils.CircularBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Basic broker implementation using rendez-vous pattern for channel connections.
 * Accept and connect calls sync up on the same port to establish bidirectional channels.
 */
public class LocalBroker extends Broker {
    // Shared state for rendez-vous - keyed by "brokerName:port"
    private static Map<String, RendezVous> pendingConnections = new HashMap<>();

    private static final int BUFFER_SIZE = 1024;

    public LocalBroker(String name) {
        super(name);
        BrokerManager.getInstance().addBroker(name, this);
    }

    // Blocks until someone connects to this port
    @Override
    public synchronized Channel accept(int port) {
        String key = name + ":" + port;

        synchronized (pendingConnections) {
            RendezVous rv = pendingConnections.get(key);

            if (rv != null && rv.connectWaiting) {
                // Connect already waiting - create channel pair
                pendingConnections.remove(key);

                ChannelImpl acceptChannel = new ChannelImpl(rv.bufferBA, rv.bufferAB);
                rv.connectChannel = new ChannelImpl(rv.bufferAB, rv.bufferBA);

                rv.ready = true;
                pendingConnections.notifyAll();

                return acceptChannel;
            } else {
                // Wait for matching connect
                rv = new RendezVous();
                rv.acceptWaiting = true;
                rv.bufferAB = new CircularBuffer(BUFFER_SIZE);
                rv.bufferBA = new CircularBuffer(BUFFER_SIZE);
                pendingConnections.put(key, rv);

                while (!rv.ready) {
                    try {
                        pendingConnections.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }

                pendingConnections.remove(key);
                return rv.acceptChannel;
            }
        }
    }

    @Override
    public synchronized Channel connect(String remoteName, int port) {
        Broker remoteBroker = BrokerManager.getInstance().getBroker(remoteName);
        if (remoteBroker == null) {
            return null;
        }

        String key = remoteName + ":" + port;

        synchronized (pendingConnections) {
            RendezVous rv = pendingConnections.get(key);

            if (rv != null && rv.acceptWaiting) {
                // Accept already waiting - create channel pair
                pendingConnections.remove(key);

                ChannelImpl connectChannel = new ChannelImpl(rv.bufferAB, rv.bufferBA);
                rv.acceptChannel = new ChannelImpl(rv.bufferBA, rv.bufferAB);

                rv.ready = true;
                pendingConnections.notifyAll();

                return connectChannel;
            } else {
                // Wait for matching accept
                rv = new RendezVous();
                rv.connectWaiting = true;
                rv.bufferAB = new CircularBuffer(BUFFER_SIZE);
                rv.bufferBA = new CircularBuffer(BUFFER_SIZE);
                pendingConnections.put(key, rv);

                while (!rv.ready) {
                    try {
                        pendingConnections.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }

                pendingConnections.remove(key);
                return rv.connectChannel;
            }
        }
    }

    // Synchronization helper for accept/connect pairing
    private static class RendezVous {
        boolean acceptWaiting = false;
        boolean connectWaiting = false;
        boolean ready = false;

        CircularBuffer bufferAB;
        CircularBuffer bufferBA;

        ChannelImpl acceptChannel;
        ChannelImpl connectChannel;
    }
}

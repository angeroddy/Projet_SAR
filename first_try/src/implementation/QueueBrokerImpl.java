package implementation;

import abstracts.Broker;
import abstracts.Channel;
import abstracts.MessageQueue;
import abstracts.QueueBroker;

/**
 * Wraps a broker to provide message-oriented communication instead of byte streams.
 */
public class QueueBrokerImpl extends QueueBroker {

    public QueueBrokerImpl(Broker broker) {
        super(broker);
    }

    @Override
    public MessageQueue accept(int port) {
        Channel channel = broker.accept(port);
        if (channel == null) {
            return null;
        }
        return new MessageQueueImpl(channel);
    }

    @Override
    public MessageQueue connect(String name, int port) {
        Channel channel = broker.connect(name, port);
        if (channel == null) {
            return null;
        }
        return new MessageQueueImpl(channel);
    }
}

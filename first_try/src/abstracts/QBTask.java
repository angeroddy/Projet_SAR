package abstracts;

/**
 * Task variant that provides access to a QueueBroker instead of raw Broker.
 */
public abstract class QBTask extends Task {
    protected QueueBroker queueBroker;

    public QBTask(QueueBroker b, Runnable r) {
        super(b.getBroker(), r);
        this.queueBroker = b;
    }

    public static QueueBroker getQueueBroker() {
        Task task = currentTask.get();
        if (task instanceof QBTask) {
            return ((QBTask)task).queueBroker;
        }
        return null;
    }
}

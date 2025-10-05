package firs_try;

public abstract class Task extends Thread {
    protected QueueBroker queueBroker;
    protected Runnable runnable;

    public Task(QueueBroker b, Runnable r) {
        this.queueBroker = b;
        this.runnable = r;
    }

    public QueueBroker getQueueBroker() {
        return queueBroker;
    }

    @Override
    public void run() {
        runnable.run();
    }
}

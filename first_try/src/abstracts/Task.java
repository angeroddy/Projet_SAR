package abstracts;

/**
 * Associates a broker with a runnable for execution.
 * Uses ThreadLocal so runnables can access their broker context.
 */
public abstract class Task extends Thread {
    protected Broker broker;
    protected Runnable runnable;

    protected static ThreadLocal<Task> currentTask = new ThreadLocal<>();

    public Task(Broker b, Runnable r) {
        this.broker = b;
        this.runnable = r;
    }

    @Override
    public void run() {
        currentTask.set(this);
        try {
            runnable.run();
        } finally {
            currentTask.remove();
        }
    }

    // Get broker for current thread's task
    public static Broker getBroker() {
        Task task = currentTask.get();
        return task != null ? task.broker : null;
    }

    public static Task getTask() {
        return currentTask.get();
    }
}

package abstracts;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Single-threaded event processor. Handles events one at a time in order.
 * Supports delayed event posting via scheduler.
 */
public class EventPump extends Thread {
    private BlockingQueue<Runnable> eventQueue;
    private ScheduledExecutorService scheduler;
    private volatile boolean running;

    public EventPump() {
        this.eventQueue = new LinkedBlockingQueue<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.running = true;
    }

    public void postEvent(Event e) {
        postRunnable(() -> e.react());
    }

    public void postEvent(Event e, int delay) {
        postRunnable(() -> e.react(), delay);
    }

    public void post(Runnable r) {
        postRunnable(r);
    }

    public void post(Runnable r, int delay) {
        postRunnable(r, delay);
    }

    private void postRunnable(Runnable r) {
        if (running) {
            eventQueue.offer(r);
        }
    }

    private void postRunnable(Runnable r, int delay) {
        if (running && delay > 0) {
            scheduler.schedule(() -> postRunnable(r), delay, TimeUnit.MILLISECONDS);
        } else {
            postRunnable(r);
        }
    }

    @Override
    public void run() {
        while (running) {
            try {
                Runnable event = eventQueue.take();
                event.run();
            } catch (InterruptedException e) {
                if (!running) {
                    break;
                }
            }
        }
    }

    public void shutdown() {
        running = false;
        scheduler.shutdown();
        this.interrupt();
    }

    public boolean isRunning() {
        return running;
    }
}

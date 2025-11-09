package implementation;

import abstracts.Broker;
import abstracts.Task;

public class TaskImpl extends Task {

    public TaskImpl(Broker b, Runnable r) {
        super(b, r);
    }
}

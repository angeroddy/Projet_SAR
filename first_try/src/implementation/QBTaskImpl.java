package implementation;

import abstracts.QBTask;
import abstracts.QueueBroker;

public class QBTaskImpl extends QBTask {

    public QBTaskImpl(QueueBroker b, Runnable r) {
        super(b, r);
    }
}

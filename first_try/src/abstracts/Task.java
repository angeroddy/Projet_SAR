package abstracts;

public class Task {
	  protected Broker broker;
	    protected Runnable runnable;

	    public Task(Broker b, Runnable r) {
	        this.broker = b;
	        this.runnable = r;
	    }

	    public static Broker getBroker() {
	    	return null;
	    }
}

package implementation;

import abstracts.Broker;
import abstracts.Channel;

public class SimpleBroker extends Broker{
	public SimpleBroker(String name) {
        super(name);
    }

    @Override
    public Channel accept(int port) {
        System.out.println("Serveur en attente sur le port " + port);
        return new SimpleChannel("Server");
    }

    @Override
    public Channel connect(String name, int port) {
        System.out.println("Client se connecte à " + name + ":" + port);
        return new SimpleChannel("Client");
    }
}

package tests;

import abstracts.Broker;
import abstracts.Channel;
import implementation.SimpleBroker;

class ServerTask implements Runnable {
    @Override
    public void run() {
        Broker broker = new SimpleBroker("Server");
        Channel channel = broker.accept(8080);

        // Lire un message
        byte[] buffer = new byte[100];
        int bytesRead = channel.read(buffer, 0, buffer.length);
        String message = new String(buffer, 0, bytesRead);
        System.out.println("Serveur reçoit: " + message);

        // Répondre
        String response = "Bonjour client!";
        channel.write(response.getBytes(), 0, response.getBytes().length);

        channel.disconnect();
    }
}

// Runnable Client
class ClientTask implements Runnable {
    @Override
    public void run() {
        Broker broker = new SimpleBroker("Client");
        Channel channel = broker.connect("Server", 8080);


        String message = "Bonjour serveur!";
        channel.write(message.getBytes(), 0, message.getBytes().length);


        byte[] buffer = new byte[100];
        int bytesRead = channel.read(buffer, 0, buffer.length);
        String response = new String(buffer, 0, bytesRead);
        System.out.println("Client reçoit: " + response);

        channel.disconnect();
    }
}
public class example_test {
	  public static void main(String[] args) {
	        // Lancer serveur
	        Thread server = new Thread(new ServerTask());
	        server.start();

	        // Lancer client
	        Thread client = new Thread(new ClientTask());
	        client.start();
	        
	        try {
	            server.join();
	            client.join();
	        } catch (InterruptedException e) {
	            System.err.println("Threads interrompus : " + e.getMessage());
	        }

	        System.out.println("Test terminé.");
	    }
}

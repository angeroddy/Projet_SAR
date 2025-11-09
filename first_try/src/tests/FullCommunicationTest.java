package tests;

import abstracts.Broker;
import abstracts.Channel;
import implementation.LocalBroker;

/**
 * Test complet de la communication bidirectionnelle avec rendez-vous.
 * Teste que deux threads peuvent se connecter via accept/connect et échanger des messages.
 */
public class FullCommunicationTest {

    public static void main(String[] args) {
        System.out.println("=== Test Communication Bidirectionnelle ===\n");

        testBasicRendezVous();
        testMultipleMessages();
        testDisconnection();

        System.out.println("\n=== Tous les tests passés! ===");
    }

    /**
     * Test 1: Rendez-vous basique et échange de messages simple
     */
    private static void testBasicRendezVous() {
        System.out.println("Test 1: Rendez-vous basique");

        // Thread serveur (accept)
        Thread serverThread = new Thread(() -> {
            Broker serverBroker = new LocalBroker("ServerBroker");
            Channel channel = serverBroker.accept(8080);

            // Lire message du client
            byte[] buffer = new byte[100];
            int bytesRead = channel.read(buffer, 0, buffer.length);
            String clientMsg = new String(buffer, 0, bytesRead);
            System.out.println("  Serveur reçoit: " + clientMsg);

            // Répondre au client
            String response = "Bonjour client!";
            channel.write(response.getBytes(), 0, response.getBytes().length);

            assert clientMsg.equals("Bonjour serveur!") : "Message client incorrect";

            channel.disconnect();
        });

        // Thread client (connect)
        Thread clientThread = new Thread(() -> {
            try {
                Thread.sleep(50); // S'assurer que le serveur est prêt
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Broker clientBroker = new LocalBroker("ClientBroker");
            Channel channel = clientBroker.connect("ServerBroker", 8080);

            // Envoyer message au serveur
            String message = "Bonjour serveur!";
            channel.write(message.getBytes(), 0, message.getBytes().length);

            // Lire réponse du serveur
            byte[] buffer = new byte[100];
            int bytesRead = channel.read(buffer, 0, buffer.length);
            String serverMsg = new String(buffer, 0, bytesRead);
            System.out.println("  Client reçoit: " + serverMsg);

            assert serverMsg.equals("Bonjour client!") : "Message serveur incorrect";

            channel.disconnect();
        });

        serverThread.start();
        clientThread.start();

        try {
            serverThread.join();
            clientThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("  [OK] Rendez-vous basique OK\n");
    }

    /**
     * Test 2: Multiples messages dans les deux directions
     */
    private static void testMultipleMessages() {
        System.out.println("Test 2: Messages multiples");

        Thread serverThread = new Thread(() -> {
            Broker broker = new LocalBroker("Server2");
            Channel channel = broker.accept(9000);

            byte[] buffer = new byte[100];

            // Échanger 3 messages
            for (int i = 1; i <= 3; i++) {
                // Recevoir du client
                int bytesRead = channel.read(buffer, 0, buffer.length);
                String msg = new String(buffer, 0, bytesRead);
                System.out.println("  Serveur reçoit: " + msg);

                // Répondre au client
                String response = "ACK " + i;
                channel.write(response.getBytes(), 0, response.getBytes().length);
            }

            channel.disconnect();
        });

        Thread clientThread = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Broker broker = new LocalBroker("Client2");
            Channel channel = broker.connect("Server2", 9000);

            byte[] buffer = new byte[100];

            // Échanger 3 messages
            for (int i = 1; i <= 3; i++) {
                // Envoyer au serveur
                String message = "Message " + i;
                channel.write(message.getBytes(), 0, message.getBytes().length);

                // Recevoir ACK du serveur
                int bytesRead = channel.read(buffer, 0, buffer.length);
                String ack = new String(buffer, 0, bytesRead);
                System.out.println("  Client reçoit: " + ack);
            }

            channel.disconnect();
        });

        serverThread.start();
        clientThread.start();

        try {
            serverThread.join();
            clientThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("  [OK] Messages multiples OK\n");
    }

    /**
     * Test 3: Déconnexion et vidage des buffers
     */
    private static void testDisconnection() {
        System.out.println("Test 3: Déconnexion propre");

        Thread serverThread = new Thread(() -> {
            Broker broker = new LocalBroker("Server3");
            Channel channel = broker.accept(9001);

            // Envoyer plusieurs messages puis se déconnecter
            for (int i = 1; i <= 3; i++) {
                String msg = "Data " + i;
                channel.write(msg.getBytes(), 0, msg.getBytes().length);
            }

            channel.disconnect();
            System.out.println("  Serveur déconnecté");
        });

        Thread clientThread = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Broker broker = new LocalBroker("Client3");
            Channel channel = broker.connect("Server3", 9001);

            byte[] buffer = new byte[100];

            // Lire tous les messages même après déconnexion du serveur
            int count = 0;
            int bytesRead;
            while ((bytesRead = channel.read(buffer, 0, buffer.length)) > 0) {
                String msg = new String(buffer, 0, bytesRead);
                System.out.println("  Client reçoit: " + msg);
                count++;
            }

            assert count == 3 : "Le client devrait recevoir 3 messages";

            channel.disconnect();
        });

        serverThread.start();
        clientThread.start();

        try {
            serverThread.join();
            clientThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("  [OK] Déconnexion propre OK\n");
    }
}

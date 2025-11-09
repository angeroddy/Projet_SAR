package tests;

import abstracts.Broker;
import abstracts.MessageQueue;
import abstracts.QBTask;
import abstracts.QueueBroker;
import implementation.LocalBroker;
import implementation.QBTaskImpl;
import implementation.QueueBrokerImpl;

/**
 * Test complet de MessageQueue et QueueBroker.
 * Vérifie l'envoi et la réception de messages complets (pas de fragmentation).
 */
public class MessageQueueTest {

    public static void main(String[] args) {
        System.out.println("=== Test MessageQueue & QueueBroker ===\n");

        testBasicMessageExchange();
        testMultipleMessages();
        testLargeMessages();
        testTaskIntegration();

        System.out.println("\n=== Tous les tests MessageQueue passés! ===");
    }

    /**
     * Test 1: Échange de messages simple
     */
    private static void testBasicMessageExchange() {
        System.out.println("Test 1: Échange de messages simple");

        Thread serverThread = new Thread(() -> {
            Broker broker = new LocalBroker("MQServer1");
            QueueBroker queueBroker = new QueueBrokerImpl(broker);
            MessageQueue mq = queueBroker.accept(8080);

            // Recevoir un message du client
            byte[] received = mq.receive();
            String clientMsg = new String(received);
            System.out.println("  Serveur reçoit message: \"" + clientMsg + "\"");

            // Répondre au client
            String response = "Bonjour client!";
            mq.send(response.getBytes(), 0, response.getBytes().length);

            assert clientMsg.equals("Bonjour serveur!") : "Message client incorrect";

            mq.close();
        });

        Thread clientThread = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Broker broker = new LocalBroker("MQClient1");
            QueueBroker queueBroker = new QueueBrokerImpl(broker);
            MessageQueue mq = queueBroker.connect("MQServer1", 8080);

            // Envoyer un message au serveur
            String message = "Bonjour serveur!";
            mq.send(message.getBytes(), 0, message.getBytes().length);

            // Recevoir la réponse
            byte[] received = mq.receive();
            String serverMsg = new String(received);
            System.out.println("  Client reçoit message: \"" + serverMsg + "\"");

            assert serverMsg.equals("Bonjour client!") : "Message serveur incorrect";

            mq.close();
        });

        serverThread.start();
        clientThread.start();

        try {
            serverThread.join();
            clientThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("  [OK] Échange de messages simple OK\n");
    }

    /**
     * Test 2: Multiples messages distincts
     */
    private static void testMultipleMessages() {
        System.out.println("Test 2: Messages multiples distincts");

        Thread serverThread = new Thread(() -> {
            Broker broker = new LocalBroker("MQServer2");
            QueueBroker queueBroker = new QueueBrokerImpl(broker);
            MessageQueue mq = queueBroker.accept(9000);

            // Recevoir 3 messages séparés
            for (int i = 1; i <= 3; i++) {
                byte[] received = mq.receive();
                String msg = new String(received);
                System.out.println("  Serveur reçoit: \"" + msg + "\"");
                assert msg.equals("Message " + i) : "Message incorrect";

                // Répondre avec ACK
                String ack = "ACK " + i;
                mq.send(ack.getBytes(), 0, ack.getBytes().length);
            }

            mq.close();
        });

        Thread clientThread = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Broker broker = new LocalBroker("MQClient2");
            QueueBroker queueBroker = new QueueBrokerImpl(broker);
            MessageQueue mq = queueBroker.connect("MQServer2", 9000);

            // Envoyer 3 messages séparés
            for (int i = 1; i <= 3; i++) {
                String message = "Message " + i;
                mq.send(message.getBytes(), 0, message.getBytes().length);

                // Recevoir ACK
                byte[] received = mq.receive();
                String ack = new String(received);
                System.out.println("  Client reçoit: \"" + ack + "\"");
                assert ack.equals("ACK " + i) : "ACK incorrect";
            }

            mq.close();
        });

        serverThread.start();
        clientThread.start();

        try {
            serverThread.join();
            clientThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("  [OK] Messages multiples distincts OK\n");
    }

    /**
     * Test 3: Messages de taille variable
     */
    private static void testLargeMessages() {
        System.out.println("Test 3: Messages de taille variable");

        Thread serverThread = new Thread(() -> {
            Broker broker = new LocalBroker("MQServer3");
            QueueBroker queueBroker = new QueueBrokerImpl(broker);
            MessageQueue mq = queueBroker.accept(9001);

            // Recevoir 3 messages de tailles différentes
            int[] sizes = {10, 100, 500};
            for (int size : sizes) {
                byte[] received = mq.receive();
                System.out.println("  Serveur reçoit message de " + received.length + " bytes");
                assert received.length == size : "Taille incorrecte: attendu " + size + ", reçu " + received.length;
            }

            mq.close();
        });

        Thread clientThread = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Broker broker = new LocalBroker("MQClient3");
            QueueBroker queueBroker = new QueueBrokerImpl(broker);
            MessageQueue mq = queueBroker.connect("MQServer3", 9001);

            // Envoyer 3 messages de tailles différentes
            int[] sizes = {10, 100, 500};
            for (int size : sizes) {
                byte[] message = new byte[size];
                for (int i = 0; i < size; i++) {
                    message[i] = (byte) ('A' + (i % 26));
                }
                mq.send(message, 0, message.length);
                System.out.println("  Client envoie message de " + size + " bytes");
            }

            mq.close();
        });

        serverThread.start();
        clientThread.start();

        try {
            serverThread.join();
            clientThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("  [OK] Messages de taille variable OK\n");
    }

    /**
     * Test 4: Intégration avec Task et ThreadLocal
     */
    private static void testTaskIntegration() {
        System.out.println("Test 4: Intégration Task + ThreadLocal");

        // Runnable serveur qui utilise QBTask.getQueueBroker()
        Runnable serverRunnable = () -> {
            QueueBroker qb = QBTask.getQueueBroker();
            assert qb != null : "QueueBroker devrait être accessible via QBTask.getQueueBroker()";

            MessageQueue mq = qb.accept(9002);

            byte[] received = mq.receive();
            String msg = new String(received);
            System.out.println("  Serveur (QBTask) reçoit: \"" + msg + "\"");

            mq.close();
        };

        // Runnable client qui utilise QBTask.getQueueBroker()
        Runnable clientRunnable = () -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            QueueBroker qb = QBTask.getQueueBroker();
            assert qb != null : "QueueBroker devrait être accessible via QBTask.getQueueBroker()";

            MessageQueue mq = qb.connect("MQServer4", 9002);

            String message = "Hello via QBTask!";
            mq.send(message.getBytes(), 0, message.getBytes().length);
            System.out.println("  Client (QBTask) envoie: \"" + message + "\"");

            mq.close();
        };

        // Créer des QBTasks avec QueueBroker
        Broker serverBroker = new LocalBroker("MQServer4");
        QueueBroker serverQueueBroker = new QueueBrokerImpl(serverBroker);
        QBTaskImpl serverTask = new QBTaskImpl(serverQueueBroker, serverRunnable);

        Broker clientBroker = new LocalBroker("MQClient4");
        QueueBroker clientQueueBroker = new QueueBrokerImpl(clientBroker);
        QBTaskImpl clientTask = new QBTaskImpl(clientQueueBroker, clientRunnable);

        serverTask.start();
        clientTask.start();

        try {
            serverTask.join();
            clientTask.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("  [OK] Intégration Task + ThreadLocal OK\n");
    }
}

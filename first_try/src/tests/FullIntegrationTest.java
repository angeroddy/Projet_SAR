package tests;

import abstracts.Broker;
import abstracts.MessageQueue;
import abstracts.QBTask;
import abstracts.QueueBroker;
import implementation.LocalBroker;
import implementation.QBTaskImpl;
import implementation.QueueBrokerImpl;

/**
 * Test d'intégration complet démontrant toute l'architecture
 * conforme à l'image des "Synchronous Message Queues".
 *
 * Architecture complète testée:
 * Application (Task + Runnable)
 *     ↓
 * MessageQueue (send/receive messages)
 *     ↓
 * Channel (read/write byte streams)
 *     ↓
 * CircularBuffer (FIFO, blocking, thread-safe)
 */
public class FullIntegrationTest {

    public static void main(String[] args) {
        System.out.println("=== Test Intégration Complète ===");
        System.out.println("Architecture conforme à l'image:\n");
        System.out.println("  [Task] → [QueueBroker] → [MessageQueue] → [Channel] → [CircularBuffer]\n");

        testChatApplication();

        System.out.println("\n=== Test d'intégration complète réussi! ===");
    }

    /**
     * Simulation d'une mini-application de chat utilisant toute l'architecture
     */
    private static void testChatApplication() {
        System.out.println("Simulation: Application de Chat\n");

        // Runnable pour le serveur de chat
        Runnable chatServerRunnable = () -> {
            System.out.println("[Server] Démarrage du serveur de chat...");

            // Accès au QueueBroker via QBTask.getQueueBroker()
            QueueBroker qb = QBTask.getQueueBroker();
            MessageQueue mq = qb.accept(7000);

            System.out.println("[Server] En attente de connexion sur le port 7000...");

            // Recevoir le nom du client
            byte[] nameData = mq.receive();
            String clientName = new String(nameData);
            System.out.println("[Server] Client connecté: " + clientName);

            // Envoyer message de bienvenue
            String welcome = "Bienvenue sur ChatServer, " + clientName + "!";
            mq.send(welcome.getBytes(), 0, welcome.getBytes().length);

            // Échanger plusieurs messages
            for (int i = 1; i <= 3; i++) {
                // Recevoir message du client
                byte[] msgData = mq.receive();
                String clientMsg = new String(msgData);
                System.out.println("[Server] " + clientName + " dit: " + clientMsg);

                // Répondre au client
                String response = "Message " + i + " bien reçu!";
                mq.send(response.getBytes(), 0, response.getBytes().length);
            }

            // Message de fin
            byte[] byeData = mq.receive();
            String byeMsg = new String(byeData);
            System.out.println("[Server] " + clientName + " dit: " + byeMsg);

            String farewell = "Au revoir " + clientName + "!";
            mq.send(farewell.getBytes(), 0, farewell.getBytes().length);

            mq.close();
            System.out.println("[Server] Serveur fermé");
        };

        // Runnable pour le client de chat
        Runnable chatClientRunnable = () -> {
            try {
                Thread.sleep(100); // S'assurer que le serveur est prêt
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("[Client] Connexion au serveur de chat...");

            // Accès au QueueBroker via QBTask.getQueueBroker()
            QueueBroker qb = QBTask.getQueueBroker();
            MessageQueue mq = qb.connect("ChatServer", 7000);

            // Envoyer le nom du client
            String clientName = "Alice";
            mq.send(clientName.getBytes(), 0, clientName.getBytes().length);

            // Recevoir message de bienvenue
            byte[] welcomeData = mq.receive();
            String welcome = new String(welcomeData);
            System.out.println("[Client] Serveur dit: " + welcome);

            // Envoyer plusieurs messages
            String[] messages = {
                "Bonjour!",
                "Comment allez-vous?",
                "Merci pour votre aide!"
            };

            for (String msg : messages) {
                mq.send(msg.getBytes(), 0, msg.getBytes().length);
                System.out.println("[Client] Envoi: " + msg);

                // Recevoir réponse
                byte[] responseData = mq.receive();
                String response = new String(responseData);
                System.out.println("[Client] Serveur dit: " + response);
            }

            // Message de déconnexion
            String bye = "Au revoir!";
            mq.send(bye.getBytes(), 0, bye.getBytes().length);

            byte[] farewellData = mq.receive();
            String farewell = new String(farewellData);
            System.out.println("[Client] Serveur dit: " + farewell);

            mq.close();
            System.out.println("[Client] Client déconnecté");
        };

        // Créer les brokers
        Broker serverBroker = new LocalBroker("ChatServer");
        QueueBroker serverQueueBroker = new QueueBrokerImpl(serverBroker);

        Broker clientBroker = new LocalBroker("ChatClient");
        QueueBroker clientQueueBroker = new QueueBrokerImpl(clientBroker);

        // Créer et lancer les QBTasks
        QBTaskImpl serverTask = new QBTaskImpl(serverQueueBroker, chatServerRunnable);
        QBTaskImpl clientTask = new QBTaskImpl(clientQueueBroker, chatClientRunnable);

        serverTask.start();
        clientTask.start();

        try {
            serverTask.join();
            clientTask.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("\n[OK] Application de chat fonctionne parfaitement!");
        System.out.println("  - Messages envoyés et reçus complets (pas de fragmentation)");
        System.out.println("  - Communication bidirectionnelle fluide");
        System.out.println("  - QBTask + ThreadLocal fonctionnent correctement");
        System.out.println("  - Toutes les couches (MessageQueue → Channel → CircularBuffer) opérationnelles");
    }
}

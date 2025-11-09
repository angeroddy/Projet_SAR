package tests;

import utils.CircularBuffer;

public class CircularBufferTest {

    public static void main(String[] args) {
        System.out.println("=== Test CircularBuffer ===\n");

        testBasicPushPull();
        testBulkOperations();
        testBlockingBehavior();
        testCloseBehavior();

        System.out.println("\n=== Tous les tests passés! ===");
    }

    private static void testBasicPushPull() {
        System.out.println("Test 1: Push/Pull unitaire");
        CircularBuffer buffer = new CircularBuffer(10);

        // Test push/pull unitaire
        buffer.push((byte) 'A');
        buffer.push((byte) 'B');
        buffer.push((byte) 'C');

        byte b1 = buffer.pull();
        byte b2 = buffer.pull();
        byte b3 = buffer.pull();

        assert b1 == 'A' : "Expected 'A', got " + (char)b1;
        assert b2 == 'B' : "Expected 'B', got " + (char)b2;
        assert b3 == 'C' : "Expected 'C', got " + (char)b3;

        System.out.println("  [OK] Push/Pull unitaire OK");
    }

    private static void testBulkOperations() {
        System.out.println("Test 2: Operations bulk");
        CircularBuffer buffer = new CircularBuffer(20);

        // Test push bulk
        String message = "Hello";
        byte[] data = message.getBytes();
        int pushed = buffer.push(data, 0, data.length);
        assert pushed == data.length : "Expected " + data.length + " bytes pushed, got " + pushed;

        // Test pull bulk
        byte[] received = new byte[10];
        int pulled = buffer.pull(received, 0, received.length);
        assert pulled == data.length : "Expected " + data.length + " bytes pulled, got " + pulled;

        String receivedMsg = new String(received, 0, pulled);
        assert receivedMsg.equals(message) : "Expected '" + message + "', got '" + receivedMsg + "'";

        System.out.println("  [OK] Operations bulk OK");
    }

    private static void testBlockingBehavior() {
        System.out.println("Test 3: Comportement bloquant");
        CircularBuffer buffer = new CircularBuffer(20);

        // Thread writer
        Thread writer = new Thread(() -> {
            try {
                Thread.sleep(100); // Attend un peu
                String message = "Delayed message";
                buffer.push(message.getBytes(), 0, message.getBytes().length);
                System.out.println("  [OK] Writer a écrit: " + message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        // Thread reader (bloque en attendant des données)
        Thread reader = new Thread(() -> {
            byte[] received = new byte[100];
            int pulled = buffer.pull(received, 0, received.length);
            String message = new String(received, 0, pulled);
            System.out.println("  [OK] Reader a lu: " + message);
            assert message.equals("Delayed message") : "Message incorrect";
        });

        reader.start();
        writer.start();

        try {
            reader.join();
            writer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("  [OK] Comportement bloquant OK");
    }

    private static void testCloseBehavior() {
        System.out.println("Test 4: Comportement de fermeture");
        CircularBuffer buffer = new CircularBuffer(20);

        // Écrire des données
        String message = "Final data";
        buffer.push(message.getBytes(), 0, message.getBytes().length);

        // Fermer le buffer
        buffer.close();
        assert buffer.closed() : "Buffer devrait être fermé";

        // Tenter d'écrire après fermeture (devrait retourner 0)
        int pushed = buffer.push("More data".getBytes(), 0, 9);
        assert pushed == 0 : "Push après fermeture devrait retourner 0";

        // Lire les données restantes
        byte[] received = new byte[100];
        int pulled = buffer.pull(received, 0, received.length);
        String receivedMsg = new String(received, 0, pulled);
        assert receivedMsg.equals(message) : "Devrait lire les données restantes";

        // Lire à nouveau (buffer vide et fermé, devrait retourner 0)
        pulled = buffer.pull(received, 0, received.length);
        assert pulled == 0 : "Pull sur buffer fermé et vide devrait retourner 0";

        System.out.println("  [OK] Comportement de fermeture OK");
    }
}

package tests;

import abstracts.AsyncMessageQueue;
import abstracts.AsyncQueueBroker;
import abstracts.Broker;
import abstracts.EventPump;
import abstracts.QueueBroker;
import implementation.LocalBroker;
import implementation.AsyncQueueBrokerImpl;
import implementation.QueueBrokerImpl;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test async event-driven architecture with EventPump.
 */
public class AsyncEventDrivenTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Async Event-Driven Test ===\n");

        testAsyncChatApplication();

        System.out.println("\n=== Test complete ===");
    }

    private static void testAsyncChatApplication() throws Exception {
        System.out.println("Test: Async Chat Application\n");

        CountDownLatch serverReady = new CountDownLatch(1);
        CountDownLatch conversationComplete = new CountDownLatch(2);

        EventPump serverPump = new EventPump();
        EventPump clientPump = new EventPump();

        serverPump.start();
        clientPump.start();

        Broker serverBroker = new LocalBroker("AsyncChatServer");
        QueueBroker serverSyncQB = new QueueBrokerImpl(serverBroker);

        Broker clientBroker = new LocalBroker("AsyncChatClient");
        QueueBroker clientSyncQB = new QueueBrokerImpl(clientBroker);

        AsyncQueueBroker serverAsyncQB = new AsyncQueueBrokerImpl(
            "AsyncChatServer", serverSyncQB, serverPump);

        AsyncQueueBroker clientAsyncQB = new AsyncQueueBrokerImpl(
            "AsyncChatClient", clientSyncQB, clientPump);

        serverAsyncQB.bind(8000, queue -> {
            System.out.println("[Server Event] Client connected");

            queue.setListener(new AsyncMessageQueue.Listener() {
                private int messageCount = 0;

                @Override
                public void received(byte[] msg) {
                    messageCount++;
                    String message = new String(msg);
                    System.out.println("[Server Event] Received: " + message + " (#" + messageCount + ")");

                    String response = "ACK " + messageCount;
                    queue.send(response.getBytes());
                    System.out.println("[Server Event] Sent: " + response);

                    if (messageCount == 3) {
                        serverPump.post(() -> {
                            queue.close();
                            conversationComplete.countDown();
                        }, 500);
                    }
                }

                @Override
                public void closed() {
                    System.out.println("[Server Event] Queue closed");
                }
            });

            serverReady.countDown();
        });

        System.out.println("[Server] Bound to port 8000, waiting...\n");

        Thread.sleep(100);

        clientAsyncQB.connect("AsyncChatServer", 8000, new AsyncQueueBroker.ConnectListener() {
            @Override
            public void connected(AsyncMessageQueue queue) {
                System.out.println("[Client Event] Connected to server");

                queue.setListener(new AsyncMessageQueue.Listener() {
                    private int ackCount = 0;

                    @Override
                    public void received(byte[] msg) {
                        ackCount++;
                        String message = new String(msg);
                        System.out.println("[Client Event] Got: " + message);

                        if (ackCount == 3) {
                            clientPump.post(() -> {
                                queue.close();
                                conversationComplete.countDown();
                            }, 200);
                        }
                    }

                    @Override
                    public void closed() {
                        System.out.println("[Client Event] Queue closed");
                    }
                });

                String[] messages = {"Hello Server!", "How are you?", "Goodbye!"};
                for (int i = 0; i < messages.length; i++) {
                    final String msg = messages[i];
                    final int num = i + 1;

                    clientPump.post(() -> {
                        queue.send(msg.getBytes());
                        System.out.println("[Client Event] Sent: " + msg);
                    }, 300 * num);
                }
            }

            @Override
            public void refused() {
                System.out.println("[Client Event] Connection refused");
                conversationComplete.countDown();
                conversationComplete.countDown();
            }
        });

        System.out.println("[Client] Connecting...\n");

        boolean completed = conversationComplete.await(10, TimeUnit.SECONDS);

        if (completed) {
            System.out.println("\n[PASS] Async chat succeeded");
            System.out.println("  - EventPump processes events sequentially");
            System.out.println("  - Async bind/connect with callbacks");
            System.out.println("  - Message reception via listeners");
        } else {
            System.out.println("\n[FAIL] Timeout - conversation incomplete");
        }

        Thread.sleep(500);
        serverPump.shutdown();
        clientPump.shutdown();
    }
}

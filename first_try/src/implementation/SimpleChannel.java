package implementation;

import abstracts.Channel;

public class SimpleChannel extends Channel {
    private String name;
    private boolean connected = true;
    private String lastMessage = "";

    public SimpleChannel(String name) {
        this.name = name;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
        if (name.equals("Server")) {
            String message = "Hello from client!";
            byte[] data = message.getBytes();
            System.arraycopy(data, 0, bytes, offset, Math.min(data.length, length));
            return data.length;
        } else {
            String message = "Hello from server!";
            byte[] data = message.getBytes();
            System.arraycopy(data, 0, bytes, offset, Math.min(data.length, length));
            return data.length;
        }
    }

    @Override
    public int write(byte[] bytes, int offset, int length) {
        String message = new String(bytes, offset, length);
        System.out.println(name + " envoie: " + message);
        return length;
    }

    @Override
    public void disconnect() {
        connected = false;
        System.out.println(name + " déconnecté");
    }

    @Override
    public boolean disconnected() {
        return !connected;
    }
}

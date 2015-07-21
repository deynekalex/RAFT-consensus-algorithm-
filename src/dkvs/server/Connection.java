package dkvs.server;

import dkvs.server.messages.Message;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Scanner;

public class Connection {
    private final Socket socket;
    private final ConnectionReader reader;
    private final ConnectionWriter writer;
    private volatile boolean isClosed;

    public Connection(Socket socket, NodeThread thread) throws IOException {
        this.socket = socket;
        reader = new ConnectionReader(socket, thread);
        writer = new ConnectionWriter(socket);
    }

    public void send(Message message) {
        writer.add(message);
    }

    public void start() {
        reader.start();
        writer.start();
    }

    public void close() {
        isClosed = true;
        try {
            socket.close();
        } catch (IOException e) {
        }
        writer.interrupt();
    }

    public boolean isClosed() {
        return isClosed;
    }

    private class ConnectionReader extends Thread {
        final Scanner scanner;
        final NodeThread thread;

        public ConnectionReader(Socket socket, NodeThread thread) throws IOException {
            scanner = new Scanner(socket.getInputStream());
            this.thread = thread;
        }

        @Override
        public void run() {
            while (!isClosed) {
                try {
                    String next = scanner.next();
                    if (next.equals("message")) {
                        Message message = Message.parseMessage(socket.getRemoteSocketAddress(), scanner);
                        if (message != null) {
                            thread.add(message);
                        } else {
                            close();
                        }
                    } else {
                        close();
                    }
                } catch (NoSuchElementException | IllegalStateException e) {
                    close();
                }
            }
        }
    }

    private class ConnectionWriter extends Thread {
        final PrintWriter writer;
        final Queue<Message> output = new ArrayDeque<>();

        public ConnectionWriter(Socket socket) throws IOException {
            writer = new PrintWriter(socket.getOutputStream());
        }

        @Override
        public void run() {
            while (!isClosed) {
                Message message = null;
                while (message == null) {
                    synchronized (output) {
                        if (!output.isEmpty()) {
                            message = output.poll();
                        } else {
                            try {
                                output.wait();
                            } catch (InterruptedException e) {
                                return;
                            }
                        }
                    }
                }
                writer.write("message " + message + "\n");
                writer.flush();
            }
        }

        public void add(Message message) {
            synchronized (output) {
                output.add(message);
                output.notify();
            }
        }
    }
}

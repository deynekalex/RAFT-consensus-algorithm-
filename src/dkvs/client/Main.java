package dkvs.client;

import dkvs.server.messages.Message;
import dkvs.server.messages.csp.ClientServerRequest;
import dkvs.server.messages.csp.ClientServerResponse;
import dkvs.server.util.Operation;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        Socket socket;
        boolean connected = false;
        Scanner input = null;
        PrintWriter output = null;
        while (true) {
            System.out.print("> ");
            String cmd = sc.next();
            switch (cmd) {
                case "connect":
                    try {
                        if (connected) {
                            output.write("end");
                            output.close();
                            input.close();
                            connected = false;
                        }
                        socket = new Socket();
                        socket.connect(new InetSocketAddress(sc.next(), sc.nextInt()));
                        input = new Scanner(socket.getInputStream());
                        output = new PrintWriter(socket.getOutputStream());
                        connected = true;
                    } catch (IOException e) {
                        System.err.println("Error while connecting: " + e.getMessage());
                        System.err.flush();
                    }
                    break;
                case "exit":
                    if (connected) {
                        output.write("end");
                        output.close();
                        input.close();
                    }
                    return;
                case "ping":
                    if (connected) {
                        send(output, new ClientServerRequest(null, Operation.PING, null, null, new ArrayList<>()));
                        Message m = receive(input);
                        if (m != null && m instanceof ClientServerResponse && ((ClientServerResponse) m).success) {
                            System.out.println("pong");
                        } else {
                            System.out.println("Something bad has happened.");
                        }
                    } else {
                        System.out.println("Not connected!");
                    }
                    break;
                case "get":
                    if (connected) {
                        send(output, new ClientServerRequest(null, Operation.GET, sc.next(), null, new ArrayList<>()));
                        Message m = receive(input);
                        if (m != null && m instanceof ClientServerResponse) {
                            if (((ClientServerResponse) m).success) {
                                System.out.println(((ClientServerResponse) m).result);
                            } else {
                                System.out.println("fail: " + (((ClientServerResponse) m).result.equals("?") ? "not found" : ((ClientServerResponse) m).result));
                            }
                        } else {
                            System.out.println("Something bad has happened.");
                        }
                    } else {
                        System.out.println("Not connected!");
                    }
                    break;
                case "set":
                    if (connected) {
                        send(output, new ClientServerRequest(null, Operation.SET, sc.next(), sc.nextLine().substring(1), new ArrayList<>()));
                        Message m = receive(input);
                        if (m != null && m instanceof ClientServerResponse) {
                            if (((ClientServerResponse) m).success) {
                                System.out.println("stored");
                            } else {
                                System.out.println("fail: " + ((ClientServerResponse) m).result);
                            }
                        } else {
                            System.out.println("Something bad has happened.");
                        }
                    } else {
                        System.out.println("Not connected!");
                    }
                    break;
                case "delete":
                    if (connected) {
                        send(output, new ClientServerRequest(null, Operation.DELETE, sc.next(), null, new ArrayList<>()));
                        Message m = receive(input);
                        if (m != null && m instanceof ClientServerResponse) {
                            if (((ClientServerResponse) m).success) {
                                System.out.println("deleted");
                            } else {
                                System.out.println("fail: " + (((ClientServerResponse) m).result.equals("?") ? "not found" : ((ClientServerResponse) m).result));
                            }
                        } else {
                            System.out.println("Something bad has happened.");
                        }
                    } else {
                        System.out.println("Not connected!");
                    }
                    break;
                default:
                    System.out.println("Unrecognized command!");
            }
        }
    }

    private static void send(PrintWriter writer, Message message) {
        writer.write("message " + message + "\n");
        writer.flush();
    }

    private static Message receive(Scanner sc) {
        String next = sc.next();
        if (next.equals("message")) {
            return Message.parseMessage(null, sc);
        } else {
            return null;
        }
    }
}

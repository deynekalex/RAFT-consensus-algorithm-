package dkvs.server.messages.csp;

import dkvs.server.messages.Message;
import dkvs.server.util.Operation;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class ClientServerRequest extends Message {
    public final Operation operation;
    public final String key, value;
    public final List<InetSocketAddress> redirections;

    public ClientServerRequest(SocketAddress address, Operation operation, String key, String value, List<InetSocketAddress> redirections) {
        super(address);
        this.operation = operation;
        this.key = key;
        this.value = value;
        this.redirections = redirections;
    }

    public static ClientServerRequest parseMessage(SocketAddress address, Scanner sc) {
        return new ClientServerRequest(address, Operation.valueOf(sc.next()), sc.next(), sc.nextLine().substring(1), getRedirections(sc));
    }

    private static List<InetSocketAddress> getRedirections(Scanner sc) {
        List<InetSocketAddress> redirections = new ArrayList<>();
        int n = sc.nextInt();
        for (int i = 0; i < n; i++) {
            redirections.add(new InetSocketAddress(sc.next(), sc.nextInt()));
        }
        return redirections;
    }

    @Override
    public String prettyPrint() {
        String result = "ClientRequest { operation=" + operation +
                ", key=" + (key == null ? "?" : key) + ", value=" + (value == null ? "?" : value) + ", redirections=[";
        for (int i = 0; i < redirections.size(); i++) {
            result += redirections.get(i) + (i == redirections.size() - 1 ? "" : ", ");
        }
        return result + "] }";
    }

    @Override
    public String toString() {
        String result = "client_request " + operation + " " + (key == null ? "?" : key) +
                " " + (value == null ? "?" : value) + "\n" + redirections.size() + " ";
        for (InetSocketAddress redirection : redirections) {
            result += redirection.getHostName() + " " + redirection.getPort() + " ";
        }
        return result;
    }
}

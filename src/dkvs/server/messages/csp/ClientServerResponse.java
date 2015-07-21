package dkvs.server.messages.csp;

import dkvs.server.messages.Message;
import dkvs.server.util.Operation;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class ClientServerResponse extends Message {
    public final Operation operation;
    public final boolean success;
    public final String result;
    public final List<InetSocketAddress> redirections;

    public ClientServerResponse(SocketAddress address, Operation operation, boolean success, String result, List<InetSocketAddress> redirections) {
        super(address);
        this.operation = operation;
        this.success = success;
        this.result = result;
        this.redirections = redirections;
    }

    public static ClientServerResponse parseMessage(SocketAddress address, Scanner sc) {
        return new ClientServerResponse(address, Operation.valueOf(sc.next()), sc.nextBoolean(), sc.nextLine().substring(1), getRedirections(sc));
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
        String res = "ServerResponse { operation=" + operation + ", success=" + success +
                ", result=" + (result == null ? "?" : result) + ", redirections=[";
        for (int i = 0; i < redirections.size(); i++) {
            res += redirections.get(i) + (i == redirections.size() - 1 ? "" : ", ");
        }
        return res + "] }";
    }

    @Override
    public String toString() {
        String res = "server_response " + operation + " " + success + " " + (result == null ? "?" : result) + "\n" + redirections.size() + " ";
        for (InetSocketAddress redirection : redirections) {
            res += redirection.getHostName() + " " + redirection.getPort() + " ";
        }
        return res;
    }
}

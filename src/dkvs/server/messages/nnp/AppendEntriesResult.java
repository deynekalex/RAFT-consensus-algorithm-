package dkvs.server.messages.nnp;

import dkvs.server.messages.Message;

import java.net.SocketAddress;
import java.util.Scanner;

public class AppendEntriesResult extends Message {
    public final int term;
    public final boolean success;
    public final int currentLength;
    public final int id;

    public AppendEntriesResult(SocketAddress address, int term, boolean success, int currentLength, int id) {
        super(address);
        this.term = term;
        this.success = success;
        this.currentLength = currentLength;
        this.id = id;
    }

    public static AppendEntriesResult parseMessage(SocketAddress address, Scanner sc) {
        return new AppendEntriesResult(address, sc.nextInt(), sc.nextBoolean(), sc.nextInt(), sc.nextInt());
    }

    @Override
    public String prettyPrint() {
        return "AppendResult { term=" + term + ", success=" + success + ", currentLength=" + currentLength + ", id=" + id + " }";
    }

    @Override
    public String toString() {
        return "append_result " + term + " " + success + " " + currentLength + " " + id;
    }
}

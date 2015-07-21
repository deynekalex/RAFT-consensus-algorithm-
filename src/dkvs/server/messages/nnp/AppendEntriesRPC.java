package dkvs.server.messages.nnp;

import dkvs.server.messages.Message;
import dkvs.server.util.Entry;

import java.net.SocketAddress;
import java.util.Scanner;

public class AppendEntriesRPC extends Message {
    public final int term;
    public final int leaderId;
    public final int prevLogIndex;
    public final int prevLogTerm;
    public final int leaderCommit;
    public final Entry[] log;

    public AppendEntriesRPC(SocketAddress address, int term, int leaderId, int prevLogIndex, int prevLogTerm, int leaderCommit, Entry[] log) {
        super(address);
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.log = log;
        this.leaderCommit = leaderCommit;
    }

    public static AppendEntriesRPC parseMessage(SocketAddress address, Scanner sc) {
        return new AppendEntriesRPC(address, sc.nextInt(), sc.nextInt(), sc.nextInt(), sc.nextInt(), sc.nextInt(), getLog(sc));
    }

    private static Entry[] getLog(Scanner sc) {
        int n = sc.nextInt();
        Entry[] log = new Entry[n];
        for (int i = 0; i < n; i++) {
            log[i] = Entry.parseEntry(sc);
        }
        return log;
    }

    @Override
    public String prettyPrint() {
        String result = "AppendRequest { term=" + term + ", leaderId=" + leaderId + ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm + ", leaderCommit=" + leaderCommit + ", log=[";
        for (int i = 0; i < log.length; i++) {
            result += log[i].prettyPrint() + (i == log.length - 1 ? "" : ", ");
        }
        return result + "] }";
    }

    @Override
    public String toString() {
        String result = "append_request " + term + " " + leaderId + " " + prevLogIndex + " " + prevLogTerm + " " + leaderCommit + " " + log.length + " ";
        for (Entry entry : log) {
            result += entry + "\n";
        }
        return result;
    }
}

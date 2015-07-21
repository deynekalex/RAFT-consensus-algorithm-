package dkvs.server.messages.nnp;

import dkvs.server.messages.Message;

import java.net.SocketAddress;
import java.util.Scanner;

public class RequestVoteRPC extends Message {
    public final int term;
    public final int candidateId;
    public final int lastLogIndex;
    public final int lastLogTerm;

    public RequestVoteRPC(SocketAddress address, int term, int candidateId, int lastLogIndex, int lastLogTerm) {
        super(address);
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public static RequestVoteRPC parseMessage(SocketAddress address, Scanner sc) {
        return new RequestVoteRPC(address, sc.nextInt(), sc.nextInt(), sc.nextInt(), sc.nextInt());
    }

    @Override
    public String prettyPrint() {
        return "VoteRequest { term=" + term + ", candidateId=" + candidateId + ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm + " }";
    }

    @Override
    public String toString() {
        return "vote_request " + term + " " + candidateId + " " + lastLogIndex + " " + lastLogTerm;
    }
}

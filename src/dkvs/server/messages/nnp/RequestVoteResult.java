package dkvs.server.messages.nnp;

import dkvs.server.messages.Message;

import java.net.SocketAddress;
import java.util.Scanner;

public class RequestVoteResult extends Message {
    public final int term;
    public final boolean voteGranted;

    public RequestVoteResult(SocketAddress address, int term, boolean voteGranted) {
        super(address);
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public static RequestVoteResult parseMessage(SocketAddress address, Scanner sc) {
        return new RequestVoteResult(address, sc.nextInt(), sc.nextBoolean());
    }

    @Override
    public String prettyPrint() {
        return "VoteRequestResult { term=" + term + ", voteGranted=" + voteGranted + " }";
    }

    @Override
    public String toString() {
        return "vote_result " + term + " " + voteGranted;
    }
}

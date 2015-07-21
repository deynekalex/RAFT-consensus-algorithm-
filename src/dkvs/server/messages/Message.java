package dkvs.server.messages;

import dkvs.server.messages.csp.ClientServerRequest;
import dkvs.server.messages.csp.ClientServerResponse;
import dkvs.server.messages.nnp.AppendEntriesRPC;
import dkvs.server.messages.nnp.AppendEntriesResult;
import dkvs.server.messages.nnp.RequestVoteRPC;
import dkvs.server.messages.nnp.RequestVoteResult;

import java.net.SocketAddress;
import java.util.Scanner;

public abstract class Message {
    public final SocketAddress address;

    public Message(SocketAddress address) {
        this.address = address;
    }

    public static Message parseMessage(SocketAddress address, Scanner sc) {
        String s = sc.next();
        switch (s) {
            case "client_request":
                return ClientServerRequest.parseMessage(address, sc);
            case "server_response":
                return ClientServerResponse.parseMessage(address, sc);
            case "vote_request":
                return RequestVoteRPC.parseMessage(address, sc);
            case "vote_result":
                return RequestVoteResult.parseMessage(address, sc);
            case "append_request":
                return AppendEntriesRPC.parseMessage(address, sc);
            case "append_result":
                return AppendEntriesResult.parseMessage(address, sc);
            default:
                return null;
        }
    }

    public abstract String prettyPrint();
}

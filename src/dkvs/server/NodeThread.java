package dkvs.server;

import dkvs.server.messages.Message;
import dkvs.server.messages.csp.ClientServerRequest;
import dkvs.server.messages.csp.ClientServerResponse;
import dkvs.server.messages.nnp.AppendEntriesRPC;
import dkvs.server.messages.nnp.AppendEntriesResult;
import dkvs.server.messages.nnp.RequestVoteRPC;
import dkvs.server.messages.nnp.RequestVoteResult;
import dkvs.server.util.Configuration;
import dkvs.server.util.Entry;
import dkvs.server.util.Operation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;

public class NodeThread extends Thread {
    private final Configuration configuration;
    private final ConnectionManager manager;
    private final Queue<Message> input = new ArrayDeque<>();
    private final Queue<Message> output = new ArrayDeque<>();

    private final RSM rsm;
    private final State state;

    private volatile boolean isStopped;

    public NodeThread(Configuration configuration) throws IOException {
        this.configuration = configuration;
        manager = new ConnectionManager(configuration, this);
        rsm = new RSM(configuration);
        state = new State();
        state.commitNext = rsm.size();
        if (configuration.number == 0) {
            state.state = 2;
        }
    }

    public void add(Message message) {
        synchronized (input) {
            input.add(message);
            input.notify();
        }
    }

    public Message get() throws InterruptedException {
        while (true) {
            synchronized (output) {
                if (!output.isEmpty()) {
                    return output.poll();
                }
                output.wait();
            }
        }
    }

    private Message getMessage() throws InterruptedException {
        int timeout = getTimeout();
        while (true) {
            synchronized (input) {
                if (!input.isEmpty()) {
                    return input.poll();
                } else if (System.currentTimeMillis() - state.lastMessageTime >= timeout) {
                    return null;
                } else {
                    input.wait(timeout - (System.currentTimeMillis() - state.lastMessageTime));
                }
            }
        }
    }

    @Override
    public void run() {
        manager.start();
        state.lastMessageTime = System.currentTimeMillis();
        while (!isStopped) {
            try {
                Message message = getMessage();
                if (message == null) {
                    processTimeout();
                } else {
                    processMessage(message);
                }
            } catch (InterruptedException | IOException e) {
                try {
                    close();
                } catch (IOException e1) {
                    System.err.println("Error while closing RSM.");
                }
                return;
            }
        }
    }

    private void processMessage(Message message) throws IOException {
        System.out.println(getStatus() + " Message received (" + message.address + "): " + message.prettyPrint());
        if (message instanceof ClientServerRequest) {
            processClientServerRequest((ClientServerRequest) message);
        } else if (message instanceof ClientServerResponse) {
            processClientServerResponse((ClientServerResponse) message);
        } else if (message instanceof AppendEntriesRPC) {
            processAppendEntriesRPC((AppendEntriesRPC) message);
        } else if (message instanceof AppendEntriesResult) {
            processAppendEntriesResult((AppendEntriesResult) message);
        } else if (message instanceof RequestVoteRPC) {
            processRequestVoteRPC((RequestVoteRPC) message);
        } else if (message instanceof RequestVoteResult) {
            processRequestVoteResult((RequestVoteResult) message);
        } else {
            throw new AssertionError();
        }
    }

    private void processClientServerRequest(ClientServerRequest request) throws IOException {
        switch (request.operation) {
            case PING:
                sendVerbose(new ClientServerResponse(request.address, Operation.PING, true, null, request.redirections));
                return;
            case GET:
                sendVerbose(new ClientServerResponse(request.address, Operation.GET,
                        rsm.containsKey(request.key), rsm.get(request.key), request.redirections));
                return;
            case SET:
            case DELETE:
                if (state.state == 2) {
                    rsm.add(new Entry(state.term, request.operation, request.key, request.value), request);
                    checkCommit();
                    processTimeout();
                } else if (state.leader != -1) {
                    ClientServerRequest r = new ClientServerRequest(
                            new InetSocketAddress(configuration.hosts[state.leader], configuration.ports[state.leader]),
                            request.operation, request.key, request.value, request.redirections);
                    r.redirections.add((InetSocketAddress) request.address);
                    sendVerbose(r);
                } else {
                    sendVerbose(new ClientServerResponse(request.address, request.operation, false, "Unknown leader", request.redirections));
                }
                return;
            default:
                throw new AssertionError();
        }
    }

    private void processClientServerResponse(ClientServerResponse response) {
        ClientServerResponse r = new ClientServerResponse(response.redirections.get(response.redirections.size() - 1),
                response.operation, response.success, response.result, response.redirections);
        r.redirections.remove(r.redirections.size() - 1);
        sendVerbose(r);
    }

    private void processAppendEntriesRPC(AppendEntriesRPC rpc) throws IOException {
        if (rpc.term >= state.term) {
            if (state.state != 0) {
                System.out.println("Converting to follower.");
            }
            state.setTerm(rpc.term);
            state.state = 0;
            state.lastMessageTime = System.currentTimeMillis();
            state.leader = rpc.leaderId;
        }
        if (state.term == rpc.term && (rpc.prevLogIndex == -1
                || rsm.size() > rpc.prevLogIndex && rsm.get(rpc.prevLogIndex).term == rpc.prevLogTerm)) {
            rsm.remove(rpc.prevLogIndex);
            rsm.add(rpc.log);
            if (rpc.leaderCommit > state.commitNext) {
                commit(Math.min(rpc.leaderCommit, rsm.size()));
            }
            sendVerbose(new AppendEntriesResult(rpc.address, state.term, true, rsm.size(), configuration.number));
        } else {
            sendVerbose(new AppendEntriesResult(rpc.address, state.term, false, rsm.size(), configuration.number));
        }
    }

    private void processAppendEntriesResult(AppendEntriesResult result) throws IOException {
        if (result.success) {
            state.nextIndex[result.id] = result.currentLength;
            state.matchIndex[result.id] = result.currentLength;
            checkCommit();
        } else {
            state.nextIndex[result.id]--;
        }
    }

    private void processRequestVoteRPC(RequestVoteRPC rpc) throws IOException {
        if (rpc.term < state.term) {
            sendVerbose(new RequestVoteResult(rpc.address, state.term, false));
        } else {
            if (rpc.term > state.term) {
                if (state.state != 0) {
                    System.out.println("Converting to follower.");
                }
                state.setTerm(rpc.term);
                state.setVotedFor(-1);
                state.state = 0;
            }
            int llt = rsm.size() == 0 ? -2 : rsm.get(rsm.size() - 1).term;
            boolean upd = rpc.lastLogTerm > llt || rpc.lastLogTerm == llt && rpc.lastLogIndex >= rsm.size() - 1;
            if (upd && (state.votedFor == -1 || state.votedFor == rpc.candidateId)) {
                state.setVotedFor(rpc.candidateId);
                sendVerbose(new RequestVoteResult(rpc.address, state.term, true));
            } else {
                sendVerbose(new RequestVoteResult(rpc.address, state.term, false));
            }
        }
    }

    private void processRequestVoteResult(RequestVoteResult result) throws IOException {
        if (state.state == 1) {
            if (result.term == state.term && result.voteGranted) {
                state.voteCount++;
                if (state.voteCount > configuration.totalNumber / 2) {
                    System.out.println("Converting to leader.");
                    state.state = 2;
                    for (int i = 0; i < state.nextIndex.length; i++) {
                        state.nextIndex[i] = rsm.size();
                        state.matchIndex[i] = 0;
                    }
                    processTimeout();
                }
            } else if (result.term > state.term) {
                System.out.println("Converting to follower.");
                state.setTerm(result.term);
                state.setVotedFor(-1);
                state.state = 0;
            }
        }
    }

    private void processTimeout() throws IOException {
        if (state.state == 2) {
            for (int i = 0; i < configuration.totalNumber; i++) {
                if (i != configuration.number) {
                    Entry[] entries = new Entry[rsm.size() - state.nextIndex[i]];
                    for (int j = 0; j < entries.length; j++) {
                        entries[j] = rsm.get(state.nextIndex[i] + j);
                    }
                    sendVerbose(new AppendEntriesRPC(
                            new InetSocketAddress(configuration.hosts[i], configuration.ports[i]),
                            state.term, configuration.number, state.nextIndex[i] - 1,
                            state.nextIndex[i] == 0 ? -1 : rsm.get(state.nextIndex[i] - 1).term,
                            state.commitNext, entries));
                }
            }
        } else {
            if (state.state != 1) {
                System.out.println("Converting to candidate.");
            }
            state.leader = -1;
            state.setTerm(state.term + 1);
            state.state = 1;
            state.setVotedFor(configuration.number);
            state.voteCount = 1;
            if (configuration.totalNumber == 1) {
                state.state = 2;
                state.nextIndex[0] = rsm.size();
                state.matchIndex[0] = 0;
                System.out.println("Converting to leader.");
            }
            for (int i = 0; i < configuration.totalNumber; i++) {
                if (i != configuration.number) {
                    sendVerbose(new RequestVoteRPC(
                            new InetSocketAddress(configuration.hosts[i], configuration.ports[i]),
                            state.term, configuration.number, rsm.size() - 1,
                            rsm.size() == 0 ? -1 : rsm.get(rsm.size() - 1).term));
                }
            }
        }
        state.lastMessageTime = System.currentTimeMillis();
    }

    private void checkCommit() throws IOException {
        for (int i = state.commitNext; i < rsm.size(); i++) {
            int num = 1;
            for (int j = 0; j < configuration.totalNumber; j++) {
                if (configuration.number != j && state.matchIndex[j] > i) {
                    num++;
                }
            }
            if (num > configuration.totalNumber / 2 && rsm.get(i).term == state.term) {
                commit(i + 1);
            }
        }
    }

    private int getTimeout() {
        return state.state == 2 ? configuration.timeout / 2 : configuration.timeout;
    }

    public void close() throws IOException {
        isStopped = true;
        manager.close();
        rsm.close();
        interrupt();
    }

    private void commit(int commitNext) throws IOException {
        List<ClientServerResponse> responses = state.commit(commitNext);
        responses.forEach(this::sendVerbose);
    }

    private void sendVerbose(Message message) {
        System.out.println(getStatus() + " Sending message (" + message.address + "): " + message.prettyPrint());
        synchronized (output) {
            output.add(message);
            output.notify();
        }
    }

    private String getStatus() {
        return "[" + (state.state == 0 ? "FOLLOWER" : (state.state == 1 ? "CANDIDATE" : "LEADER")) + ", term=" +
                state.term + "]";
    }

    private class State {
        final int[] nextIndex, matchIndex;
        int state;
        int commitNext;
        int term, votedFor;
        long lastMessageTime;
        int voteCount;
        int leader = -1;

        private String stateFile;

        public State() throws IOException {
            nextIndex = new int[configuration.totalNumber];
            for (int i = 0; i < nextIndex.length; i++) {
                nextIndex[i] = rsm.size();
            }
            matchIndex = new int[configuration.totalNumber];
            stateFile = ".state_" + (configuration.number + 1);
            readState();
        }

        public void setTerm(int term) throws IOException {
            this.term = term;
            writeState();
        }

        public void setVotedFor(int votedFor) throws IOException {
            this.votedFor = votedFor;
            writeState();
        }

        public List<ClientServerResponse> commit(int commitNext) throws IOException {
            List<ClientServerResponse> responses = rsm.commit(this.commitNext, commitNext);
            this.commitNext = commitNext;
            return responses;
        }

        private void readState() {
            try {
                Scanner sc = new Scanner(new File(stateFile));
                term = sc.nextInt();
                votedFor = sc.nextInt();
                sc.close();
            } catch (FileNotFoundException e) {
                votedFor = -1;
            }
        }

        private void writeState() throws IOException {
            FileWriter writer = new FileWriter(stateFile);
            writer.write(term + " " + votedFor);
            writer.close();
        }
    }
}

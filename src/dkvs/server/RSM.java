package dkvs.server;

import dkvs.server.messages.csp.ClientServerRequest;
import dkvs.server.messages.csp.ClientServerResponse;
import dkvs.server.util.Configuration;
import dkvs.server.util.Entry;
import dkvs.server.util.Operation;

import java.io.*;
import java.util.*;

public class RSM {
    private final Map<String, String> data = new HashMap<>();
    private final List<Entry> log = new ArrayList<>();
    private final Writer writer;
    private final Map<Integer, ClientServerRequest> requests = new HashMap<>();

    public RSM(Configuration configuration) throws IOException {
        String logFile = "dkvs_" + (configuration.number + 1) + ".log";
        writer = new FileWriter(logFile, true);
        readLog(logFile);
    }

    public void add(Entry entry, ClientServerRequest request) {
        log.add(entry);
        requests.put(log.size() - 1, request);
    }

    public void add(Entry[] entries) {
        Collections.addAll(log, entries);
    }

    public int size() {
        return log.size();
    }

    public Entry get(int i) {
        return log.get(i);
    }

    public void remove(int last) {
        for (int i = last + 1; i < log.size(); i++) {
            requests.remove(i);
        }
        while (log.size() > last + 1) {
            log.remove(log.size() - 1);
        }
    }

    public boolean containsKey(String key) {
        return data.containsKey(key);
    }

    public String get(String key) {
        return data.get(key);
    }

    public List<ClientServerResponse> commit(int lastApplied, int commitNext) throws IOException {
        List<ClientServerResponse> responses = new ArrayList<>();
        for (int i = lastApplied; i < commitNext; i++) {
            ClientServerResponse response = applyEntry(i);
            if (response != null) {
                responses.add(response);
            }
            writer.write(log.get(i) + "\n");
        }
        writer.flush();
        return responses;
    }

    public void close() throws IOException {
        writer.close();
    }

    private void readLog(String fileName) throws FileNotFoundException {
        Scanner sc = new Scanner(new File(fileName));
        while (sc.hasNext()) {
            log.add(Entry.parseEntry(sc));
            applyEntry(log.size() - 1);
        }
        sc.close();
    }

    private ClientServerResponse applyEntry(int number) {
        Entry entry = log.get(number);
        if (entry.operation.equals(Operation.SET)) {
            data.put(entry.key, entry.value);
            if (requests.containsKey(number)) {
                return new ClientServerResponse(requests.get(number).address, requests.get(number).operation,
                        true, null, requests.get(number).redirections);
            }
        } else if (entry.operation.equals(Operation.DELETE)) {
            boolean success = data.containsKey(entry.key);
            data.remove(entry.key);
            if (requests.containsKey(number)) {
                return new ClientServerResponse(requests.get(number).address, requests.get(number).operation,
                        success, null, requests.get(number).redirections);
            }
        }
        return null;
    }
}

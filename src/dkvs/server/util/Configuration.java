package dkvs.server.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Configuration {
    public final int totalNumber;
    public final int number;
    public final int timeout;
    public final String[] hosts;
    public final int[] ports;

    public Configuration(String fileName, int number) throws FileNotFoundException, ParseException {
        this.number = number;
        Scanner sc = new Scanner(new File(fileName));
        Map<String, String> options = new HashMap<>();
        while (sc.hasNext()) {
            String line = sc.nextLine();
            int p = line.indexOf('=');
            if (p == -1) {
                throw new ParseException("Error: Unrecognized option: " + line);
            }
            options.put(line.substring(0, p), line.substring(p + 1));
        }
        try {
            totalNumber = Integer.parseInt(get("number", options));
            timeout = Integer.parseInt(get("timeout", options));
            hosts = new String[totalNumber];
            ports = new int[totalNumber];
            for (int i = 0; i < totalNumber; i++) {
                String s = get("node." + (i + 1), options);
                int p = s.indexOf(':');
                if (p == -1) {
                    throw new ParseException("Error: Unrecognized option: " + s);
                }
                hosts[i] = s.substring(0, p);
                ports[i] = Integer.parseInt(s.substring(p + 1));
            }
        } catch (NumberFormatException e) {
            throw new ParseException("Error: Unrecognized number!");
        }
    }

    private String get(String key, Map<String, String> options) throws ParseException {
        if (!options.containsKey(key)) {
            throw new ParseException("Error: Undefined option: " + key);
        }
        return options.get(key);
    }
}

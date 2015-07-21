package dkvs.server;

import dkvs.server.util.Configuration;
import dkvs.server.util.ParseException;

import java.io.FileNotFoundException;
import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Error: Unexpected number of arguments!");
        } else {
            try {
                Configuration configuration = new Configuration("dkvs.properties", Integer.parseInt(args[0]) - 1);
                new NodeThread(configuration).start();
            } catch (NumberFormatException e) {
                System.err.println("Error: Unrecognized node number!");
            } catch (ParseException | FileNotFoundException e) {
                System.err.println("Error while reading properties file: " + e.getMessage());
            } catch (IOException e) {
                System.err.println("Error while initializing: " + e.getMessage());
            }
        }
    }
}
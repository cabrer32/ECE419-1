package app_kvECS;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import ecs.ECS;
import ecs.ECSNode;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ECSClient implements IECSClient {
    private static Logger logger =  Logger.getRootLogger();
    private static final String[] CACHE_STRATEGY = {"LRU", "FIFO", "LFU", "None"};
    private static final String PROMPT = "B9ECS> ";
    private BufferedReader stdin;
    private boolean stop = false;
    private boolean running = false;
    private ECS ecs;
    private String configFileName;
    private CountDownLatch semaphore;
    Collection<IECSNode> serversTaken;

    public ECSClient(String configFileName) {
        this.configFileName = configFileName;
        ecs = new ECS(configFileName);
        ecs.initServerRepo();
    }

    @Override
    public boolean start() {
        // TODO
        if(running == false) {
            ecs.start();
        } else {
//            semaphore = new CountDownLatch(1);
        }
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        Collection<IECSNode> serversTaken = addNodes(1, cacheStrategy, cacheSize);
        return serversTaken.iterator().next();
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        Collection<IECSNode> serversTaken = this.setupNodes(count, cacheStrategy, cacheSize);

        if (serversTaken != null) {
            for (Iterator<IECSNode> iterator = serversTaken.iterator(); iterator.hasNext();) {
                ecs.executeScript((ECSNode) iterator.next());
            }
        } else {
            logger.info("Not enough servers available for allocation!");
        }
        try {
            this.awaitNodes(count, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.serversTaken = serversTaken;
        return serversTaken;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        TreeSet<IECSNode> serversTaken = ecs.arrangeECSNodes(count, cacheStrategy, cacheSize);
        ecs.registerWatchEvent(serversTaken);
        return serversTaken;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        ecs.awaitNodes(count, timeout);
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
//        ecs.removeECSNodes(nodeNames);
//        ecs.updataZKRoot();
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    public void run() {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    public void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("start")){
            this.start();

        } else if(tokens[0].equals("stop")) {
            this.stop();

        } else if(tokens[0].equals("shutdown")) {
            this.shutdown();

        } else if(tokens[0].equals("addnode")) {
            if(tokens.length == 3) {
                String cacheStrategy = tokens[1];
                if (Arrays.asList(CACHE_STRATEGY).contains(cacheStrategy)) {
                    int cacheSize = Integer.valueOf(tokens[2]);
                    this.addNode(cacheStrategy, cacheSize);
                } else {
                    printError("Unknown cache strategy");
                }
            } else {
                printError("Usage: addnode <count> <cache strategy> <cache size>");
            }

        } else if(tokens[0].equals("addnodes")) {
            if(tokens.length == 4) {
                int count = Integer.valueOf(tokens[1]);
                String cacheStrategy = tokens[2];
                if (Arrays.asList(CACHE_STRATEGY).contains(cacheStrategy)) {
                    int cacheSize = Integer.valueOf(tokens[3]);
                    this.addNodes(count, cacheStrategy, cacheSize);
                } else {
                    printError("Unknown cache strategy");
                }
            } else {
                printError("Usage: addnode <count> <cache strategy> <cache size>");
            }

        } else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if(tokens[0].equals("help")) {
            printHelp();

        } else if(tokens[0].equals("quit")) {
            stop = true;
//            disconnect();
            System.out.println(PROMPT + "Application exit!");

        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("B9 CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t put <key> <value> pair to the server \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t get the <value> of <key> from the server \n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private String setLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    public static void main(String[] args) {
        // TODO
        try {
            new logger.LogSetup("logs/ecs.log", Level.ALL);
            if (args.length != 1) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: ECS <configuration file>!");
            } else {
                String configFileName = args[0];
                File f = new File(configFileName);
                if(!f.exists() || f.isDirectory()) {
                    System.out.println("Error! Incorrect file path!");
                    System.exit(1);
                }
                ECSClient ecsClient =  new ECSClient(configFileName);
                ecsClient.run();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}

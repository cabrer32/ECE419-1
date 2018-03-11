package app_kvServer;

import app_kvServer.Cache.*;

import common.module.ServerThread;
import ecs.ECSNode;
import ecs.IECSNode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class KVServer implements IKVServer {

    public enum KVServerState {
        STOPPED, RUNNING, LOCKED
    }

    /**
     * logging
     */
    private static Logger logger = Logger.getRootLogger();

    /**
     * Server Information
     */
    private String name, zkHostname;
    private int port, zkPort, cacheSize;
    private ServerSocket serverSocket;
    private CacheStrategy cacheStrategy;

    /**
     * Server State
     */
    private KVServerState state;

    /**
     * Server running flag;
     */
    private boolean running;

    /**
     * List of Client Connections
     */
    private ArrayList<ClientConnection> connections;

    /**
     * cache
     */
    private KVCache cache;

    /**
     * file system database
     */
    private KVDB db;

    /**
     * zookeeper
     */
    private KVServerWatcher zkWatch;

    /**
     * metadata
     */
    ArrayList<ECSNode> serversTaken;


    /**
     * Start KV Server with selected name
     *
     * @param name       unique name of server
     * @param zkHostname hostname where zookeeper is running
     * @param zkPort     port where zookeeper is running
     */

    public KVServer(String name, String zkHostname, int zkPort) {
        this.name = name;
        this.zkHostname = zkHostname;
        this.zkPort = zkPort;

    }

    public void initKVServer(int port, int cacheSize, String Strategy) {
        logger.info("Initialize server ...");


        //initialize some local variable
        state = KVServerState.STOPPED;
        connections = new ArrayList<>();
        cacheStrategy = CacheStrategy.valueOf(Strategy);
        this.port = port;
        this.cacheSize = cacheSize;

        //initialize KVServer
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " + serverSocket.getLocalPort());
            port = serverSocket.getLocalPort();

        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return;
        }

        //Initialize cache
        switch (cacheStrategy) {
            case LRU:
                cache = new LRUCache(cacheSize);
                break;
            case FIFO:
                cache = new FIFOCache(cacheSize);
                break;
            case LFU:
                cache = new LFUCache(cacheSize);
                break;
            case None:
                break;
            default:
                logger.error("Invalid Cache Strategy!");
                throw new IllegalArgumentException("Invalid Cache Strategy!");
        }

        //Initialize file system database
        try {
            logger.info("Creating DB " + name);
            db = new KVDB(name);

        } catch (IOException e) {
            logger.error("Cannot create new DB " + e);
            return;
        }


        //Initialize server watch
        this.zkWatch = new KVServerWatcher(logger, this, this.zkHostname + ":" + this.zkPort, this.name);
        this.zkWatch.init();

        running = true;
    }

    public ArrayList<ECSNode> getMetaData() {
        return serversTaken;
    }

    public String getName() {
        return name;
    }

    public void setMetaData(ArrayList<ECSNode> serversTaken) {
        this.serversTaken = serversTaken;
    }

    public String[] getRange() {
        for (IECSNode node : serversTaken)
            if (node.getNodeName().equals(name))
                return node.getNodeHashRange();

        return null;
    }

    public void DBput(String key, String value) {
        try {
            logger.info("put to DB " + key);
            db.putKV(key, value);
        } catch (IOException e) {
            logger.error("Cannot write to file " + e);
        }

    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getHostname() {
        try {
            InetAddress myHost = InetAddress.getLocalHost();
            return myHost.getHostName();
        } catch (UnknownHostException ex) {
            logger.error("Error! Unknown Host. \n", ex);
            return null;
        }
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        return cacheStrategy;
    }

    @Override
    public int getCacheSize() {
        return cacheSize;
    }

    @Override
    public boolean inStorage(String key) {
        try {
            String result = db.getKV(key);
            boolean in = (result != null);

            if (in) logger.info("KEY: " + key + " is in STORAGE.");

            else logger.info("KEY: " + key + " is not in STORAGE.");

            return in;
        } catch (IOException e) {
            logger.error("Cannot execute IO operations" + e);
            return false;
        }
    }

    @Override
    public boolean inCache(String key) {

        if (cacheStrategy == CacheStrategy.None) return false;

        boolean in = cache.getKV(key) != null;

        if (in) logger.info("KEY: " + key + " is in CACHE.");

        else logger.info("KEY: " + key + " is not in CACHE.");

        return in;
    }

    @Override
    public String getKV(String key) throws Exception {
        if (getCacheStrategy() != CacheStrategy.None) {

            String cacheValue = cache.getKV(key);
            if (cacheValue != null) {
                logger.info("KV Operation (GET) in CACHE, KEY => " + key + ", VALUE => " + cacheValue);
                return cacheValue;
            }
            logger.info("KV Operation (GET) in CACHE, KEY => " + key + ", NOT FOUND ");
        }

        String value = db.getKV(key);

        if (value != null) {

            logger.info("KV Operation (GET) in STORAGE: KEY => " + key + ", VALUE => " + value);
            if (getCacheStrategy() != CacheStrategy.None)
                cache.putKV(key, value);
            return value;
        }

        logger.info("KV Operation (GET) in STORAGE: KEY => " + key + ", NOT FOUND");

        return value;
    }

    @Override
    public void putKV(String key, String value) throws Exception {

        if (getCacheStrategy() != CacheStrategy.None) {
            logger.info("KV Operation (PUT) in CACHE: KEY => " + key + ", VALUE => " + value);
            cache.putKV(key, value);
        }

        db.putKV(key, value);
        logger.info("KV Operation (PUT) in STORAGE: KEY => " + key + ", VALUE => " + value);
    }

    @Override
    public void clearCache() {
        logger.info("CACHE cleaning");
        if (cacheStrategy != CacheStrategy.None)
            cache.clear();
        logger.info("CACHE cleaned");
    }

    @Override
    public void clearStorage() {
        try {
            clearCache();
            logger.info("STORAGE  cleaning.");
            db.clear();
            logger.info("STORAGE  cleaned.");
        } catch (IOException e) {
            logger.error("Cannot clear Storage. ");
        }
    }

    @Override
    public void run() {

        if (serverSocket != null) {

            while (running) {
                try {
                    Socket client = serverSocket.accept();

                    ClientConnection connection = new ClientConnection(client, this);
                    new Thread(connection).start();

                    connections.add(connection);

                    logger.info("Connected to "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. " + e);
                }
            }
        }

        logger.info("Server stopped.");
    }

    @Override
    public void kill() {
        running = false;
        try {
            logger.info("Killing Server!");
            for (ClientConnection client : connections) {
                client.stop();
            }
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close sockets on port: " + port, e);
        }
    }

    @Override
    public void close() {
        running = false;
        try {
            logger.info("Closing server!");
            for (ClientConnection client : connections) {
                client.stop();
            }
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close sockets on port: " + port, e);
        }
    }

    @Override
    public void start() {
        state = KVServerState.RUNNING;
        logger.info("Start server ...");
    }

    @Override
    public void stop() {
        state = KVServerState.STOPPED;
    }

    @Override
    public void lockWrite() {
        state = KVServerState.LOCKED;
    }

    @Override
    public void unlockWrite() {
        state = KVServerState.RUNNING;
    }

    public KVServerState getState() {
        return state;
    }

    @Override
    public boolean moveData(String[] hashRange, String targetName) throws Exception {
        try {
            HashMap<String, String> map = db.getRangeKV(hashRange);

            Iterator it = map.entrySet().iterator();

            logger.info("Start transer data");
            while (it.hasNext()) {
                Map.Entry<String, String> pair = (Map.Entry) it.next();
                zkWatch.moveData(pair.getKey(), pair.getValue(), targetName);
            }

            logger.info("Start removing data");
            db.removeRangeKV(hashRange);
            logger.info("Finished removing data");

        } catch (Exception e) {
            logger.info("cannot get ranged kV");
        }

        return false;
    }

    public static void main(String[] args) {
        try {
            if (args.length != 6) {
                System.out.println("Invalid argument! Usage: Server <name> <zkHostname> <zkPort>!");
            } else {
                new logger.LogSetup("logs/server/" + args[0] + ".log", Level.ALL);

                KVServer server = new KVServer(args[0], args[1], Integer.parseInt(args[2]));

                server.initKVServer(Integer.parseInt(args[3]), Integer.parseInt(args[5]), args[4]);

                new ServerThread(server).start();

            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            logger.error("Error! Unable to initialize logger!");
            System.exit(1);
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid arguments! Usage: Server <name> <zkHostname> <zkPort>!");
            System.exit(1);
        }
    }
}

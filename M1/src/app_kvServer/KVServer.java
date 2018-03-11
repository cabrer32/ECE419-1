package app_kvServer;

import app_kvServer.Cache.*;

import common.module.ServerThread;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Scanner;


public class KVServer implements IKVServer {

    private static Logger logger = Logger.getRootLogger();

    private int port;
    private int cacheSize;
    private CacheStrategy strategy;
    private ServerSocket serverSocket;
    private boolean running;


    private KVCache cache;
    private KVDB db;

    private ArrayList<ClientConnection> connections;


    public KVServer(int port, int cacheSize, String strategy) {
        this.port = port;
        this.cacheSize = cacheSize;
        this.strategy = CacheStrategy.valueOf(strategy);

        initializeServer();
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
        return strategy;
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

        if(strategy == CacheStrategy.None) return false;

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
        }

        String value = db.getKV(key);
        if (getCacheStrategy() != CacheStrategy.None && value != null) {
            cache.putKV(key, value);
        }

        logger.info("KV Operation (GET) in STORAGE: KEY => " + key + ", VALUE => " + value);

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
        logger.info("Clear Cache.");
        if (strategy != CacheStrategy.None)
            cache.clear();
    }

    @Override
    public void clearStorage() {
        try {
            clearCache();
            logger.info("Clear Storage.");
            db.clear();
        } catch (IOException e) {
            logger.error("Cannot clear Storage. ");
        }
    }

    private boolean isRunning() {
        return this.running;
    }

    @Override
    public void run() {

        if (serverSocket != null) {

            while (isRunning()) {
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
                            "Unable to establish connection. \n");
                }
            }
        }
        logger.info("Server stopped.");
    }

    @Override
    public void kill() {
        // TODO Auto-generated method stub
        running = false;
        try {
            logger.info("Killing server!");
            for (ClientConnection client : connections) {
                client.stop();
            }
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        running = false;
        try {
            logger.info("Closing server!");
            for (ClientConnection client : connections) {
                client.stop();
            }
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
//        db.close();
    }

    private boolean initializeServer() {
        connections = new ArrayList<>();
        logger.info("Initialize server ...");

        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: "
                    + serverSocket.getLocalPort());
            port = serverSocket.getLocalPort();

        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }

        switch (strategy) {
            case LRU:
                this.strategy = CacheStrategy.LRU;
                cache = new LRUCache(cacheSize);
                break;
            case FIFO:
                this.strategy = CacheStrategy.FIFO;
                cache = new FIFOCache(cacheSize);
                break;
            case LFU:
                this.strategy = CacheStrategy.LFU;
                cache = new LFUCache(cacheSize);
                break;
            case None:
                this.strategy = CacheStrategy.None;
                break;
            default:
                logger.error("Invalid Cache Strategy!");
                return false;
        }
        try {
            db = new KVDB();

        } catch (IOException e) {
            logger.error("Cannot create new DB ");
            return false;
        }

        running = true;

        return true;
    }

    public static void main(String[] args) {
        try {
            new logger.LogSetup("logs/server.log", Level.ALL);
            if (args.length != 3) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <port> <cacheSize> <strategy>!");
            } else {
                int port = Integer.parseInt(args[0]);
                int cacheSize = Integer.parseInt(args[1]);

                KVServer server = new KVServer(port, cacheSize, (args[2]));

                new ServerThread(server).start();

                Scanner reader = new Scanner(System.in);
                boolean run = true;
                while (run) {
                    run = (reader.nextInt() != 1);

                }
                server.close();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (IllegalArgumentException e) {
            System.out.println("Error! Invalid argument <port> <cacheSize> <strategy>! Not a number!");
            System.out.println("Usage: Server <port> <cacheSize> <strategy>!");
            System.exit(1);
        }
    }
}

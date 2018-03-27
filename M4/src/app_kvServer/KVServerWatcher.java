package app_kvServer;

import common.messages.KVMessage;
import common.messages.Message;
import common.messages.MetaData;
import ecs.IECSNode;
import org.apache.log4j.Level;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.log4j.Logger;


public class KVServerWatcher {

    /**
     * Define root_path of ecs node
     */
    private static final String ROOT_PATH = "/ecs";
    /**
     * Define timeout time
     */
    private static final int SESSION_TIMEOUT = 5000;

    /**
     * Define child path of corresponding ecs node
     */
    private String nodePath = null;

    /**
     * Watcher to receive meta data
     */
    private Watcher connectionWatcher = null;

    /**
     * Watcher to get server operations
     */
    private Watcher childrenWatcher = null;

    /**
     * Watcher to get transfer data
     */
    private Watcher dataWatcher = null;

    /**
     * Watcher to get transfer data
     */
    private Watcher transferWatcher = null;

    /**
     * kvserver for callback
     */
    private KVServer kvServer = null;

    /**
     * logger object
     */
    private Logger logger = null;

    /**
     * zkAddress
     */
    private String zkAddress = null;

    /**
     * server name
     */
    private String KVname = null;

    /**
     * set signal to wait successful connection
     */
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    /**
     * zookeeper object
     */
    private ZooKeeper zk = null;

    /**
     * json parser
     */
    private Gson gson;

    /***
     * writedata countDown
     */
    CountDownLatch dataSemaphore = null;

    //constructor
    KVServerWatcher(Logger logger, KVServer kvserver, String zkAddress, String name) {
        this.logger = logger;
        this.kvServer = kvserver;
        this.zkAddress = zkAddress;
        this.KVname = name;
        this.nodePath = ROOT_PATH + "/" + name;

        Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);

        gson = new Gson();
    }

    /**
     * check if path exist and reset watch
     */
    public Stat exists(String path, Watcher watcher) {
        try {
            return this.zk.exists(path, watcher);
        } catch (Exception e) {
            logger.error("Cannot check path " + path + "   " + e);
            return null;
        }
    }

    /**
     * Read data from node
     */
    public String readData(String path, Watcher watcher) {
        try {
            String data = new String(this.zk.getData(path, watcher, null));
            logger.debug("Successfully read Node from " + path);
            return data;
        } catch (Exception e) {
            logger.error("Failed to read Node from " + path);
            logger.error(e);
            return "";
        }
    }

    /**
     * update node
     */
    public boolean writeData(String path, String data) {
        try {
            this.zk.setData(path, data.getBytes(), -1);
            logger.debug("Successfully update Node at " + path);
            return true;
        } catch (Exception e) {
            logger.error("Failed to update Node at " + path);
            logger.error(e);
        }
        return false;
    }


    /**
     * Create node with path
     */

    public boolean createPath(String path, String data) {
        try {
            logger.debug("Creating node at " + path);
            this.zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            return true;
        } catch (Exception e) {
            logger.error("Failed to create new node " + path);
            logger.error(e);
            return false;
        }
    }

    /**
     * delete node
     */
    public boolean deleteNode(String path) {
        try {
            this.zk.delete(path, -1);
            logger.debug("Successfully delete Node at " + path);
            return true;
        } catch (Exception e) {
            logger.error("Failed to delete Node at " + path);
            logger.error(e);
            return false;
        }
    }


    public void releaseConnection() {
        logger.info("Start releasing connections");

        if (this.zk != null) {
            try {
                this.zk.close();
                logger.info("Successfully closed zookeeper connection ");
            } catch (InterruptedException e) {
                logger.info("Failed closed zookeeper connection ");
                e.printStackTrace();
            }
        }
    }

    public void init() {

        connectionWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                if (event == null) return;


                KeeperState keeperState = event.getState();

                EventType eventType = event.getType();

                logger.debug("ROOT watcher triggered " + event.toString());

                if (keeperState == KeeperState.SyncConnected) {
                    switch (eventType) {
                        case None:
                            logger.info("Successfully connected to zookeeper server");
                            exists(ROOT_PATH, this);
                            connectedSemaphore.countDown();
                            break;
                        case NodeDataChanged:
                            String data = readData(ROOT_PATH, this);

                            ECSCommandExcutor ex = new ECSCommandExcutor(kvServer, logger, MetaData.JsonToMeta(data),data.substring(0, 1) );

                            new Thread(ex).start();
                            break;
                        case NodeDeleted:
                            logger.info("Node Deleted");
                            kvServer.kill();
                            break;
                        default:
                            exists(ROOT_PATH, this);
                            logger.info("Change is not related");
                            break;
                    }

                } else {
                    logger.warn("Failed to connect with zookeeper server -> root node");
                }
            }
        };

        childrenWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                if (event == null) return;

                KeeperState keeperState = event.getState();

                EventType eventType = event.getType();

                String path = event.getPath();

                logger.debug("Children watcher triggered " + event.toString());

                if (keeperState == KeeperState.SyncConnected) {
                    switch (eventType) {
                        case NodeDeleted:
                            kvServer.close();
                        default:
                            exists(path, this);
                            logger.debug("Irrelevant change.");
                    }

                } else {
                    logger.warn("Failed to connect with zookeeper server -> children node");
                }
            }
        };

        dataWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                if (event == null) return;

                KeeperState keeperState = event.getState();

                EventType eventType = event.getType();

                String path = event.getPath();

                logger.debug("data watcher triggered " + event.toString());

                if (keeperState == KeeperState.SyncConnected) {
                    switch (eventType) {
                        case NodeDataChanged:

                            String data = readData(path, this);

                            if(data.equals("")){
                                logger.debug("Irrelevant change.");
                                return;
                            }

                            if(data.substring(0,3).equals("#G#")){

                                KVMessage message = gson.fromJson(data.substring(3),Message.class);

                                KVMessage response = kvServer.responseGlobalService(message);

                                writeData(path, gson.toJson(response));

                                return;
                            }

                            String[] pair = JsonToPair(data);

                            kvServer.DBput(pair[0], pair[1]);
                            logger.info("Get new KV key => " + pair[0]);


                            try{
                                CountDownLatch cl = new CountDownLatch(1);
                                cl.await(100, TimeUnit.MILLISECONDS);
                            }catch(Exception e){
                                logger.error("Cannot watch new data sema");
                            }


                            writeData(path, "");
                        default:
                            exists(path, this);
                            logger.debug("Irrelevant change.");
                    }

                } else {
                    logger.warn("Failed to connect with zookeeper server -> children node");
                }
            }
        };


        transferWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                if (event == null) return;


                KeeperState keeperState = event.getState();

                EventType eventType = event.getType();

                String path = event.getPath();

                logger.debug("Transfer watcher triggered " + event.toString());

                if (keeperState == KeeperState.SyncConnected) {
                    switch (eventType) {
                        case NodeDataChanged:
                            logger.info("Finish transfer one KV pair.");
                            dataSemaphore.countDown();
                            break;
                        default:
                            exists(path, this);
                            logger.debug("Irrelevant change.");
                            break;
                    }
                } else {
                    logger.warn("Failed to connect with zookeeper server -> data node");
                }

            }
        };

        try {
            zk = new ZooKeeper(zkAddress, SESSION_TIMEOUT, connectionWatcher);
            logger.info("Connecting to zookeeper server");

            connectedSemaphore.await();

            if(!createPath(nodePath, ""))
                signalECS();


            exists(nodePath, childrenWatcher);

            for(int i = 0; i< 10; i++)
                exists(nodePath + "/server" + i, dataWatcher);

        } catch (Exception e) {
            logger.error("Failed to process KVServer Watcher " + e);
        }
    }


    void signalECS() {
        writeData(nodePath, "");
    }





    void reRangeNewServers(MetaData meta) {

        String oldPredecessor = kvServer.predecessor;

        if (oldPredecessor != null && !meta.getPredecessor(KVname).equals(oldPredecessor)) {
            logger.info("Need to move data to new predecessors");

            ArrayList<IECSNode> targets = meta.getServerBetween(oldPredecessor, KVname);

            for (IECSNode node : targets) {

                try {
                    kvServer.moveData(node.getNodeHashRange(), node.getNodeName());
                } catch (Exception e) {
                    logger.error("Cannot move data to " + node.getNodeName() + " " + e);
                }

            }
        }

        kvServer.predecessor = meta.getPredecessor(KVname);

        signalECS();
    }

    void reRangeNewReplicas(MetaData meta) {


        ArrayList<String> oldReplica = kvServer.replicas;
        ArrayList<String> newReplica = meta.getReplica(KVname);

        ArrayList<String> list = new ArrayList<>();

        if(oldReplica == null){
            list.addAll(newReplica);
        }
        else{
            if(!(oldReplica.get(0).equals(newReplica.get(0)) || oldReplica.get(1).equals(newReplica.get(0)))){
                list.add(newReplica.get(0));
            }

            if(!(oldReplica.get(0).equals(newReplica.get(1)) || oldReplica.get(1).equals(newReplica.get(1)))){
                list.add(newReplica.get(1));
            }

        }


        for (String node : list) {

            try {
                kvServer.moveData(meta.getNode(KVname).getNodeHashRange(), node);
            } catch (Exception e) {
                logger.error("Cannot move data to " + node + " " + e);
            }
        }

        kvServer.replicas = list;

        signalECS();
    }

    void removeServer(MetaData meta) {
        if (meta.getNode(KVname) == null) {
            kvServer.close();
        }
    }


    void moveData(Map<String, String> map, String targetName) {
        String dest = ROOT_PATH + "/" + targetName + "/" + kvServer.getName();

        Iterator it = map.entrySet().iterator();


        createPath(dest,"");

        try{
            CountDownLatch cl = new CountDownLatch(1);
            cl.await(100, TimeUnit.MILLISECONDS);
        }catch(Exception e){
            logger.error("Cannot watch new data sema");
        }


        logger.info("Start transfering data to " + targetName + " with size " + map.size());

        while (it.hasNext()) {
            Map.Entry<String, String> kv = (Map.Entry<String, String>) it.next();

            logger.info("Sending key => " + kv.getKey() + " to " + targetName);

            dataSemaphore = new CountDownLatch(1);

            writeData(dest,  pairToJson(new String[]{kv.getKey(), kv.getValue()}));

            exists(dest, transferWatcher);

            try{
                if(dataSemaphore.await(1000, TimeUnit.MILLISECONDS))
                    logger.warn("Transfer time out! ");
            }catch(Exception e){
                logger.error("Cannot watch new data sema");
            }

        }

        try{
            CountDownLatch cl = new CountDownLatch(1);
            cl.await(50, TimeUnit.MILLISECONDS);
        }catch(Exception e){
            logger.error("Cannot watch new data sema");
        }

        deleteNode(dest);

        logger.info("Done!");
    }

    KVMessage gService(KVMessage message, String target){
        String dest = ROOT_PATH + "/" + target + "/" + kvServer.getName();

        createPath(dest,"");

        try{
            CountDownLatch cl = new CountDownLatch(1);
            cl.await(100, TimeUnit.MILLISECONDS);
        }catch(Exception e){
            logger.error("Cannot watch new data sema");
        }

        logger.info("Start requesting data from " + target + " with key " + message.getKey());


        dataSemaphore = new CountDownLatch(1);

        writeData(dest,  "G#G" + gson.toJson(message));

        try{
            if(dataSemaphore.await(1000, TimeUnit.MILLISECONDS))
                logger.warn("Transfer time out! ");
        }catch(Exception e){
            logger.error("Cannot watch new data sema");
        }

        String data = readData(dest, null);

        KVMessage m = gson.fromJson(data, Message.class);


        deleteNode(dest);

        logger.info("Done!");

        return m;
    }




    String[] JsonToPair(String data) {

        Type Type = new TypeToken<String[]>() {
        }.getType();

        return gson.fromJson(data, Type);
    }

    String pairToJson(String[] kv) {
        Type Type = new TypeToken<String[]>() {
        }.getType();

        return gson.toJson(kv, Type);
    }
}

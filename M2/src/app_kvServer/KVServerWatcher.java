package app_kvServer;


import common.messages.MetaData;
import ecs.ECSNode;
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
import com.google.gson.JsonSyntaxException;
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
     * Define child path of corresponding ecs node
     */
    private String dataPath = null;
    /**
     * Define child path of corresponding ecs node
     */
    private String signalPath = null;

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
        this.dataPath = ROOT_PATH + "/data";
        this.signalPath = ROOT_PATH + "/signal";

        Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);

        gson = new Gson();
    }

    /**
     * check if path exist and reset watch
     */
    public Stat exists(String path, Watcher watcher) {
        try {
            return this.zk.exists(path, true);
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
            logger.info("Successfully read Node from " + path);
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
            logger.info("Successfully update Node at " + path);
        } catch (Exception e) {
            logger.error("Failed to update Node at " + path);
            logger.error(e);
        }
        return false;
    }


    /**
     * Create node with path
     */

    public void createPath(String path, String data, Watcher watcher) {
        try {
            logger.info("Creating node at " + path);
            Stat stat = this.zk.exists(path, null);

            if (stat != null) {
                logger.info("node at path " + path + " already exists");
                deleteNode(path);
            }
            this.zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.info("Successfully create new node " + path);
            this.zk.exists(path, watcher);
        } catch (Exception e) {
            logger.error("Failed to create new node " + path);
            logger.error(e);
        }
    }

    /**
     * delete node
     */
    public void deleteNode(String path) {
        try {
            this.zk.delete(path, -1);
            logger.info("Successfully delete Node at " + path);
        } catch (Exception e) {
            logger.error("Failed to delete Node at " + path);
            logger.error(e);
        }
    }

    public void init() {

        connectionWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                if (event == null) return;


                KeeperState keeperState = event.getState();

                EventType eventType = event.getType();

                logger.info("ROOT watcher triggered " + event.toString());

                if (keeperState == KeeperState.SyncConnected) {
                    switch (eventType) {
                        case None:
                            connectedSemaphore.countDown();
                            logger.info("Successfully connected to zookeeper server");
                            exists(ROOT_PATH, this);
                            break;
                        case NodeDataChanged:
                            logger.info("Node Changed");

                            String data = readData(ROOT_PATH, this);

                            if (data.equals("")) {
                                kvServer.stop();
                            } else {
                                if (kvServer.getState() == KVServer.KVServerState.STOPPED)
                                    kvServer.start();
                                kvServer.setMetaData(MetaData.JsonToMeta(data));
                            }
                            break;
                        case NodeDeleted:
                            logger.info("Node Deleted");
                            kvServer.kill();
                            break;
                        default:
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

                logger.info("Children watcher triggered " + event.toString());

                if (keeperState == KeeperState.SyncConnected) {
                    switch (eventType) {
                        case NodeDataChanged:
                            logger.info("Node is changed.");
                            String data = readData(nodePath, this);
                            updateServer(MetaData.JsonToMeta(data));
                            break;
                        default:
                            logger.info("Change is not related");
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

                logger.info("Data watcher triggered " + event.toString());

                if (keeperState == KeeperState.SyncConnected) {
                    switch (eventType) {
                        case NodeCreated:

                            logger.info("Node is changed.");
                            String data = readData(dataPath, null);

                            if(data.equals("")){
                                logger.info("Change is not related");
                                return;
                            }

                            Map.Entry<String, String> kv = parseJsonEntry(data);
                            kvServer.DBput(kv.getKey(), kv.getValue());

                            writeData(dataPath,"");
                            break;
                        default:
                            logger.info("Change is not related");
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

            connectedSemaphore.await(10000, TimeUnit.MILLISECONDS);

            createPath(nodePath,"",childrenWatcher);

            createPath(signalPath, "", null);

        } catch (Exception e) {
            logger.error("Failed to process KVServer Watcher " + e);
        }
    }


    void updateServer(MetaData meta) {

        if(!meta.hasServer(KVname)){
            //remove itself
            MetaData oldMeta = kvServer.getMetaData();

            String successor = oldMeta.getSuccessor(KVname);

            while(!meta.hasServer(successor))
                successor = oldMeta.getSuccessor(successor.getNodeName());



        }
        else{
            //transfer data to someone else
        }

    }


    Map.Entry<String, String> parseJsonEntry(String data) {
        Type Type = new TypeToken<Map.Entry<String, String>>() {
        }.getType();

        return gson.fromJson(data, Type);
    }

    String entryToJson(Map.Entry<String, String> kv) {
        Type Type = new TypeToken<Map.Entry<String, String>>() {
        }.getType();

        return gson.toJson(kv, Type);
    }

    void moveData(String key, String value, String targetName) {
        String dest = ROOT_PATH + "/" + targetName + "/data";

        logger.info("Sending key => " + key + " to " + targetName);

        dataSemaphore = new CountDownLatch(1);

        Map.Entry<String, String> kv = new AbstractMap.SimpleEntry<String, String>(key, value);

        writeData(dest, entryToJson(kv));
        exists(dest, dataWatcher);

        try {
            dataSemaphore.await();
        } catch (Exception e) {
            logger.error("Cannot send data ");
        }
    }
}

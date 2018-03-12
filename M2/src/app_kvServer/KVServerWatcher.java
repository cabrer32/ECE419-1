package app_kvServer;


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
    private String childPath = null;

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
        this.childPath = ROOT_PATH + "/" + name;

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

                // watch event state
                KeeperState keeperState = event.getState();
                // watch even type
                EventType eventType = event.getType();

                logger.info("connection watcher triggered " + event.toString());

                if (keeperState == KeeperState.SyncConnected) {
                    switch (eventType) {
                        case None:
                            connectedSemaphore.countDown();
                            logger.info("Successfully connected to zookeeper server");
                            exists(ROOT_PATH, this);
                            break;
                        case NodeDataChanged:
                            logger.info("ROOT_PATH data changed");

                            String data = readData(ROOT_PATH, this);

                            if (data.equals("")) {
                                kvServer.stop();
                            } else {
                                kvServer.setMetaData(parseJsonObject(data));
                            }
                            break;
                        default:
                            break;
                    }

                } else {
                    logger.warn("Failed to connect with zookeeper server");
                }

            }
        };

        childrenWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                if (event == null) return;

                // watch event state
                KeeperState keeperState = event.getState();
                // watch even type
                EventType eventType = event.getType();
                // watch event relative path
                String path = event.getPath();

                logger.info("children watcher triggered " + event.toString());

                if (keeperState == KeeperState.SyncConnected) {
                    switch (eventType) {
                        case NodeDataChanged:
                            logger.info("NODE metadata is changed.");
                            String data = readData(path, this);

                            updateServer(parseJsonObject(data));
                            break;
                        case NodeDeleted:
                            kvServer.close();

                        default:
                            break;
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

                // watch event state
                KeeperState keeperState = event.getState();
                // watch even type
                EventType eventType = event.getType();
                // watch event relative path
                String path = event.getPath();

                logger.info("data watcher triggered " + event.toString());

                if (keeperState == KeeperState.SyncConnected) {
                    switch (eventType) {
                        case NodeCreated:

                            logger.info("received new data from");

                            String data = readData(path, dataWatcher);
                            Map.Entry<String, String> kv = parseJsonEntry(data);
                            kvServer.DBput(kv.getKey(), kv.getValue());

                            deleteNode(path);
                            exists(path,this);
                            break;
                        case NodeDeleted:

                            if(path.equals(childPath + "/data"))
                            {
                                logger.info("Irrelevant");
                                return;
                            }
                            logger.info("Ack received  ");

                            if (dataSemaphore != null)
                                dataSemaphore.countDown();

                            break;
                        default:
                            break;
                    }
                } else {
                    logger.warn("Failed to connect with zookeeper server -> children node");
                }

            }
        };

        try {
            zk = new ZooKeeper(zkAddress, SESSION_TIMEOUT, connectionWatcher);
            logger.info("Connecting to zookeeper server");

            connectedSemaphore.await(10000, TimeUnit.MILLISECONDS);

            createPath(childPath, "", childrenWatcher);

            exists((childPath + "/data"), dataWatcher);

        } catch (Exception e) {
            logger.error("Failed to process KVServer Watcher " + e);
        }
    }


    void updateServer(ArrayList<ECSNode> meta) {

        //start server
        if (kvServer.getState() == KVServer.KVServerState.STOPPED) {
            kvServer.setMetaData(meta);
            kvServer.start();

            return;
        }

        String oldRange[] = kvServer.getRange();

        boolean edge = oldRange[0].compareTo(oldRange[1]) > 0;

        boolean destroy = true;


        //there are new server coming
        for (int i = 0; i < meta.size(); i++) {

            ECSNode node = meta.get(i);

            if (node.getNodeName().equals(kvServer.getName())) destroy = false;

            if (!node.getNodeName().equals(kvServer.getName())) {
                if (edge && node.getStartingHashValue().compareTo(oldRange[0]) >= 0 &&
                        node.getEndingHashValue().compareTo(oldRange[1]) <= 0) {

                    try {
                        kvServer.stop();
                        kvServer.moveData(node.getNodeHashRange(), node.getNodeName());

                    } catch (Exception e) {
                        logger.error("Error while moving data " + node.getNodeName());
                    }
                }

            }
        }


        //Need to destroy node
        if (destroy) {
            for (int i = 0; i < meta.size(); i++) {
                ECSNode node = meta.get(i);

                if (!node.getNodeName().equals(kvServer.getName())) {
                    if (edge && node.getStartingHashValue().compareTo(oldRange[0]) <= 0 &&
                            node.getEndingHashValue().compareTo(oldRange[1]) >= 0) {
                        try {
                            kvServer.lockWrite();
                            kvServer.moveData(node.getNodeHashRange(), node.getNodeName());
                            kvServer.unlockWrite();
                        } catch (Exception e) {
                            logger.error("Error while moving data to " + node.getNodeName());
                        }
                    }

                }
            }
        }

        writeData(ROOT_PATH, metaToJson(meta));
    }

    String metaToJson(ArrayList<ECSNode> meta) {
        try {
            Type listType = new TypeToken<ArrayList<ECSNode>>() {
            }.getType();
            return gson.toJson(meta, listType);
        } catch (JsonSyntaxException e) {
            logger.error("Invalid Message syntax " + e.getMessage());
        }
        return null;
    }

    ArrayList<ECSNode> parseJsonObject(String data) {
        try {
            Type listType = new TypeToken<ArrayList<ECSNode>>() {
            }.getType();
            return gson.fromJson(data, listType);
        } catch (JsonSyntaxException e) {
            logger.error("Invalid Message syntax " + e.getMessage());

        }
        return null;
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

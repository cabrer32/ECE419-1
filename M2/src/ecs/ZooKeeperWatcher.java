package ecs;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class ZooKeeperWatcher implements Watcher {

    /**
     * zookeeper
     */
    private ZooKeeper zk = null;
    private static final String PARENT_PATH = "/ecs";
    /**
     * zk children path
     */
    private static final String CHILDREN_PATH = "/ecs/";
    /**
     * signal to complete zookeeper creation
     */
    private CountDownLatch connectedSemaphore;
    /**
     * logger
     */
    private static Logger logger = Logger.getRootLogger();

    /**
     * Create ZooKeeper connection
     *
     * @param connectAddr    zookeeper address
     * @param sessionTimeout session timeout
     */
    public void createConnection(String connectAddr, int sessionTimeout) {
        this.releaseConnection();
        try {
            connectedSemaphore = new CountDownLatch(1);
            zk = new ZooKeeper(connectAddr, sessionTimeout, this);
            logger.info("Connecting to zookeeper server");
            connectedSemaphore.await();
        } catch (Exception e) {
            logger.error("Failed to connect zookeeper server " + e);
        }
    }

    /**
     * Close Zookeeper connection
     */
    public void releaseConnection() {
        if (this.zk != null) {
            try {
                this.zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Create node with path
     */
    public boolean createPath(String path, String data) {
        try {
            Stat stat = this.zk.exists(path, false);
            if (stat == null) {
                this.zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("Successfully create new node " + path);
            }
        } catch (Exception e) {
            logger.error("Failed to create new node " + path);
            logger.error(e);
            return false;
        }
        return true;
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
            return false;
        }
        return true;
    }

    /**
     * delete node
     */
    public boolean deleteNode(String path) {
        try {
            this.zk.delete(path, -1);
            logger.info("Successfully delete Node at " + path);
        } catch (Exception e) {
            logger.error("Failed to delete Node at " + path);
            logger.error(e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * 判断指定节点是否存在
     */
    public Stat exists(String path, Watcher watch) {
        try {
            return this.zk.exists(path, watch);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * get watch event
     */
    @Override
    public void process(WatchedEvent event) {

        if (event == null) {
            return;
        }

        // Event state
        KeeperState keeperState = event.getState();
        // Event type
        EventType eventType = event.getType();
        // watch event relative path
        String path = event.getPath();


        if (KeeperState.SyncConnected == keeperState) {

            if (EventType.None == eventType) {
                logger.info("Successfully connected to zookeeper");
                connectedSemaphore.countDown();
                exists(PARENT_PATH, this);
            }

            if (EventType.NodeChildrenChanged == eventType) {
                logger.info("Change has been observed in children");
                connectedSemaphore.countDown();
                exists(PARENT_PATH, this);
            }
        } else {
            logger.error("Failed to connect to zookeeper server");
        }

    }
    public void setSemaphore(int count){
        connectedSemaphore = new CountDownLatch(count);
    }

    public boolean awaitNodes(int count, int timeout) {

        boolean ifNotTimeout = true;
        try {
            ifNotTimeout = connectedSemaphore.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("Await Nodes has been interrupted!");
        }
        return ifNotTimeout;
    }

    public boolean deleteAllNodes(String rootPath, String nodePathSuffix, TreeSet<IECSNode> serverRepoTaken) {
        boolean ifAllSuccess = true;
        for(IECSNode node : serverRepoTaken) {
            ifAllSuccess = ifAllSuccess && this.deleteNode(nodePathSuffix + node.getNodeName());
        }
        ifAllSuccess = ifAllSuccess && this.deleteNode(rootPath);
        return ifAllSuccess;
    }
}

package ecs;

import com.google.gson.Gson;
import com.jcraft.jsch.HASH;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ECS {
    private static Logger logger = Logger.getRootLogger();
//    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar /Users/wuqili/Desktop/ECE419/M2/m2-server.jar %s %s %s %s %s %s &";
    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar /Users/pannnnn/UTcourses/ECE419/ece419/M2/m2-server.jar %s %s %s %s %s %s &";

    private Gson gson;
    private ZooKeeperWatcher zkWatch;
    private TreeSet<IECSNode> serverRepo = new TreeSet<>();
    private TreeSet<IECSNode> serverRepoTaken = new TreeSet<>();
    private HashMap<IECSNode, Integer> serverRepoMapping = new HashMap<>();
    private String configFileName;

    // Zookeeper specific
    private static final int SESSION_TIMEOUT = 10000;
    private static final String LOCAL_HOST = "127.0.0.1";
    private static final String CONNECTION_ADDR = "127.0.0.1:2181";
    private static final String CONNECTION_ADDR_HOST = "127.0.0.1";
    private static final String CONNECTION_ADDR_PORT = "2181";
    private static final String ROOT_PATH = "/ecs";
    private static final String NODE_PATH_SUFFIX = "/ecs/";

    /**
     * if the service is made up of any servers
     **/
//    private boolean running = false;

    public ECS(String configFileName) {
        gson = new Gson();
        this.configFileName = configFileName;
        // print heartbeat message
        Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
    }

    public void initServerRepo() {
        File configFile = new File(configFileName);
        try {
            Scanner scanner = new Scanner(configFile);
            TreeSet<ECSNode> serverRepo = new TreeSet<>();
            String name, host, hashKey;
            int port;
            ECSNode node;
            while (scanner.hasNextLine()) {
                String[] tokens = scanner.nextLine().split(" ");
                name = tokens[0];
                host = tokens[1];
                port = Integer.parseInt(tokens[2]);
                hashKey = host + ":" + String.valueOf(port);
                MessageDigest md = MessageDigest.getInstance("MD5");
                md.update(hashKey.getBytes());
                byte[] digest = md.digest();
                String startingHash = DatatypeConverter.printHexBinary(digest).toUpperCase();
                node = new ECSNode(name, host, port, startingHash);
                serverRepo.add(node);
                this.serverRepoMapping.put(node, 1);
            }
//            serverRepo.last().setEndingHashValue(serverRepo.first().getStartingHashValue());
//            Iterator itr = serverRepo.iterator();
//            ECSNode currentNode, nextNode;
//            if (itr.hasNext()) {
//                currentNode = (ECSNode) itr.next();
//                while (itr.hasNext()) {
//                    nextNode = (ECSNode) itr.next();
//                    currentNode.setEndingHashValue(nextNode.getStartingHashValue());
//                    currentNode = nextNode;
//                }
//            }
            this.serverRepo = (TreeSet<IECSNode>) serverRepo.clone();
            initZookeeper();
        } catch (FileNotFoundException e) {
            System.out.println("Error! Unable to open the file!");
            e.printStackTrace();
            System.exit(1);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        }
    }

    private void initZookeeper() {
        zkWatch = new ZooKeeperWatcher();
        zkWatch.createConnection(CONNECTION_ADDR, SESSION_TIMEOUT);
        zkWatch.createPath(ROOT_PATH, "");
        zkWatch.watchChildren();
    }

    public TreeSet<IECSNode> arrangeECSNodes(int count, String cacheStrategy, int cacheSize) {
        TreeSet<IECSNode> serversTaken = new TreeSet<>();
        int availableNodes = 0;
        for (Integer i : serverRepoMapping.values()) {
            availableNodes += i;
        }
        if (availableNodes < count) {
            return null;
        } else {
            int i = 0;
            for (IECSNode node : serverRepo) {
                if (serverRepoMapping.get(node) == 1) {
                    ((ECSNode) node).setCacheStrategy(cacheStrategy);
                    ((ECSNode) node).setCachesize(cacheSize);
                    serversTaken.add(node);
                    serverRepoMapping.put(node, 0);
                    serverRepoTaken.add(node);
                    i = i + 1;
                }
                if (i == count) {
                    break;
                }
            }
        }
        ((ECSNode) serversTaken.last()).setEndingHashValue(((ECSNode) serversTaken.first()).getStartingHashValue());
        Iterator itr = serversTaken.iterator();
        ECSNode currentNode, nextNode;
        if (itr.hasNext()) {
            currentNode = (ECSNode) itr.next();
            while (itr.hasNext()) {
                nextNode = (ECSNode) itr.next();
                currentNode.setEndingHashValue(nextNode.getStartingHashValue());
                currentNode = nextNode;
            }
        }
        return serversTaken;
    }

    public void sendMetedata(IECSNode node) {
        logger.info("Sending latest metadata to "+ node.getNodeName());
        String json = new Gson().toJson(serverRepoTaken);
        zkWatch.writeData(NODE_PATH_SUFFIX + node.getNodeName(), json);
    }

    public void startAllNodes() {
        for (IECSNode node : serverRepoTaken)
            sendMetedata(node);
    }

    public void executeScript(ECSNode node) {
        zkWatch.clearNode(NODE_PATH_SUFFIX + node.getNodeName());

        String script = String.format(SCRIPT_TEXT, LOCAL_HOST, node.getNodeName(),CONNECTION_ADDR_HOST,
                CONNECTION_ADDR_PORT, node.getNodePort(), node.getCacheStrategy(), node.getCachesize());
        Process proc;
        Runtime run = Runtime.getRuntime();
        try {
            logger.info("Running ... " + script);
            proc = run.exec(script);
        } catch (IOException e) {
            logger.error("Failed to execute script!");
        }
    }

    public boolean removeNodes(Collection<String> nodeNames) {
        boolean ifSuccess = true;
        int removedCount = 0;
        for (Iterator<String> iterator = nodeNames.iterator(); iterator.hasNext();) {
            for (IECSNode node: serverRepoTaken) {
                String nodeName = node.getNodeName();
                if (nodeName.equals(iterator.next())){
                    if (zkWatch.deleteNode(NODE_PATH_SUFFIX + nodeName)) {
                        serverRepoTaken.remove(node);
                        serverRepoMapping.put(node, 1);
                        removedCount++;
                        break;
                    }

                }
            }
        }
        if (removedCount != nodeNames.size()) {
            ifSuccess = false;
        }
        return ifSuccess;
    }



    public boolean awaitNodes(int timeout) {
        return zkWatch.awaitNodes(timeout);
    }


    public boolean stop() {
        return zkWatch.writeData(ROOT_PATH, "");
    }

    public boolean shutdown() {
        return zkWatch.deleteAllNodes(ROOT_PATH, NODE_PATH_SUFFIX, serverRepoTaken);
    }

    public void setSemaphore(int count){
        zkWatch.setSemaphore(count);
    }

    public TreeSet<IECSNode> getNodes() {
        return serverRepoTaken;
    }

    public void notifyPrecessor(Collection<IECSNode> serversTaken) {
        TreeSet<IECSNode> tmp = (TreeSet<IECSNode>) this.serverRepoTaken.clone();
        tmp.removeAll(serversTaken);
        Iterator itr1 = serversTaken.iterator();
        ECSNode node1 = null;
        ECSNode smallerNode;
        ECSNode largerNode;
        HashMap<String ,IECSNode> map = new HashMap<>();
        while (itr1.hasNext() && tmp.size() > 0) {
            smallerNode = null;
            largerNode = null;
            node1 = (ECSNode) itr1.next();
            for(IECSNode node2 : tmp) {
                if (node1.compareTo((ECSNode) node2) <= 0) {
                    largerNode = (ECSNode) node2;
                } else {
                    smallerNode = (ECSNode) node2;
                }
                if (smallerNode == null && largerNode != null) {
                    map.put(tmp.last().getNodeName(), tmp.last());
                    break;
                } else if (smallerNode != null && largerNode != null) {
                    map.put(smallerNode.getNodeName(), smallerNode);
                } else;
            }
            if(largerNode == null) {
                map.put(smallerNode.getNodeName(), smallerNode);
            }
        }
        for(IECSNode node : map.values()) {
            sendMetedata(node);
        }
    }
}

package ecs;

import com.google.gson.Gson;
import common.messages.MetaData;
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
    //private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar /m2-server.jar %s %s %s %s %s %s &";
    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar m2-server.jar %s %s %s %s %s %s &";

    private Gson gson;
    private ECSWatcher zkWatch;


    private MetaData meta;
    private TreeSet<IECSNode> avaServer = new TreeSet<>();
    private TreeSet<IECSNode> avaRepica = new TreeSet<>();



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
    public ECS(String configFileName, String repicaFileName) {
        gson = new Gson();

        loadFile(configFileName, avaServer, true);
        loadFile(repicaFileName, avaRepica, false);

        zkWatch = new ECSWatcher();
        zkWatch.init();
    }

    private void loadFile(String configFileName, TreeSet<IECSNode> servers, boolean coordinate){
        File configFile = new File(configFileName);
        try {
            Scanner scanner = new Scanner(configFile);
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
                node = new ECSNode(name, host, port, startingHash, coordinate);
                servers.add(node);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Error! Unable to open the file!");
            e.printStackTrace();
            System.exit(1);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        }
    }

    public TreeSet<IECSNode> getAvaliableServers(int count, String cacheStrategy, int cacheSize){
        if(avaServer.size() < count && avaRepica.size() < count * 2)
            return null;

        TreeSet<IECSNode> list = new TreeSet<>();


        for(int i = 0; i < count; i++){
            ECSNode node = (ECSNode) avaServer.pollFirst();
            node.setCachesize(cacheSize); node.setCacheStrategy(cacheStrategy);
            list.add(node);
        }

        for(int i = 0; i < count * 2; i++){
            ECSNode node = (ECSNode) avaRepica.pollFirst();
            node.setCachesize(cacheSize); node.setCacheStrategy(cacheStrategy);
            list.add(node);
        }

        return list;
    }











    private TreeSet<IECSNode> arrangeECSNodes(int count, String cacheStrategy, int cacheSize) {
        TreeSet<IECSNode> serverTaken = new TreeSet<>();
        int availableNodes = 0;
        for (Integer i : serverRepoMapping.values()) {
            availableNodes += i;
        }
        if (availableNodes < count) {
            return serverTaken;
        } else {
            int i = 0;
            for (IECSNode node : serverRepo) {
                if (serverRepoMapping.get(node) == 1) {
                    ((ECSNode) node).setCacheStrategy(cacheStrategy);
                    ((ECSNode) node).setCachesize(cacheSize);
                    serverTaken.add(node);
                    serverRepoTaken.add(node);
                    serverRepoMapping.put(node, 0);
                    i = i + 1;
                }
                if (i == count) {
                    break;
                }
            }
        }
        return serverTaken;
    }

    private void setHashRange(){
        String start = ((ECSNode) serverRepoTaken.last()).getStartingHashValue();
        String end = ((ECSNode) serverRepoTaken.first()).getStartingHashValue();
        ((ECSNode) serverRepoTaken.last()).setEndingHashValue(((ECSNode) serverRepoTaken.first()).getStartingHashValue());
        Iterator itr = serverRepoTaken.iterator();
        ECSNode currentNode, nextNode;
        if (itr.hasNext()) {
            currentNode = (ECSNode) itr.next();
            while (itr.hasNext()) {
                nextNode = (ECSNode) itr.next();
                currentNode.setEndingHashValue(nextNode.getStartingHashValue());
                currentNode = nextNode;
            }
        }
    }


    public void startAllNodes() {
        for (IECSNode node : metaData.getServerRepo()){
            sendMetedata(node);
        }
    }

    public void sendMetedata(IECSNode node) {
        logger.info("Sending latest metadata to " + node.getNodeName());
        zkWatch.writeData(NODE_PATH_SUFFIX + node.getNodeName(), MetaData.MetaToJson(meta));
    }


    public void initServers(TreeSet<IECSNode> list) {

        zkWatch.setSemaphore(list.size());

        for (Iterator<IECSNode> iterator = list.iterator(); iterator.hasNext(); ) {
            ECSNode node = (ECSNode)iterator.next();

            String script = String.format(SCRIPT_TEXT, LOCAL_HOST, node.getNodeName(), CONNECTION_ADDR_HOST,
                    CONNECTION_ADDR_PORT, node.getNodePort(), node.getCacheStrategy(), node.getCachesize());

            Runtime run = Runtime.getRuntime();
            try {
                logger.info("Running ... " + script);
                run.exec(script);
            } catch (IOException e) {
                logger.error("Failed to execute script!");
            }
        }
    }

    public void addMeta(TreeSet<IECSNode> list){
            meta = new MetaData(meta.getServerRepo().addAll(list);
    }

    public boolean removeNodes(Collection<String> nodeNames) {
        boolean ifSuccess = true;
        int removedCount = 0;
        for (String nodeName1 : nodeNames) {
            for (IECSNode node : serverRepoTaken) {
                String nodeName = node.getNodeName();
                if (nodeName.equals(nodeName1)) {
                    serverRepoTaken.remove(node);
                    serverRepoMapping.put(node, 1);
                    removedCount++;
                    break;
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

    public void setSemaphore(int count) {
        zkWatch.setSemaphore(count);
    }

    public TreeSet<IECSNode> getNodes() {
        return metaData.getServerRepo();
    }

    public void notifyPrecessor(Collection<IECSNode> serversTaken) {
        TreeSet<IECSNode> tmp = (TreeSet<IECSNode>) this.serverRepoTaken.clone();
        tmp.removeAll(serversTaken);
        Iterator itr1 = serversTaken.iterator();
        ECSNode node1 = null;
        ECSNode smallerNode;
        ECSNode largerNode;
        HashMap<String, IECSNode> map = new HashMap<>();
        while (itr1.hasNext() && tmp.size() > 0) {
            smallerNode = null;
            largerNode = null;
            node1 = (ECSNode) itr1.next();
            for (IECSNode node2 : tmp) {
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
                } else ;
            }
            if (largerNode == null) {
                map.put(smallerNode.getNodeName(), smallerNode);
            }
        }
        for (IECSNode node : map.values()) {
            sendMetedata(node);
        }
    }

    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        TreeSet<IECSNode> nodes = arrangeECSNodes(count, cacheStrategy, cacheSize);
        setHashRange();
        return nodes;
    }
}

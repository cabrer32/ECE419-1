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
    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar /Users/wuqili/Desktop/ECE419/M2/m2-server.jar %s %s %s %s %s %s &";

    private ECSWatcher zkWatch;

    private MetaData meta;
    private TreeSet<IECSNode> avaServer = new TreeSet<>();
    private TreeSet<IECSNode> avaRepica = new TreeSet<>();


    private static final String LOCAL_HOST = "127.0.0.1";
    private static final String CONNECTION_ADDR_HOST = "127.0.0.1";
    private static final String CONNECTION_ADDR_PORT = "2181";
    private static final String ROOT_PATH = "/ecs";
    private static final String NODE_PATH_SUFFIX = "/ecs/";

    /**
     * if the service is made up of any servers
     **/
    public ECS(String configFileName, String repicaFileName) {
        loadFile(configFileName, avaServer, true);
        loadFile(repicaFileName, avaRepica, false);

        Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);

        zkWatch = new ECSWatcher();
        zkWatch.init();

        meta = new MetaData(new TreeSet<IECSNode>());
    }

    private void loadFile(String configFileName, TreeSet<IECSNode> servers, boolean coordinate) {
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

    public TreeSet<IECSNode> getAvaliableServers(int count, String cacheStrategy, int cacheSize) {
        if (avaServer.size() < count && avaRepica.size() < count * 2)
            return null;

        TreeSet<IECSNode> list = new TreeSet<>();


        for (int i = 0; i < count; i++) {
            ECSNode node = (ECSNode) avaServer.pollFirst();
            node.setCachesize(cacheSize);
            node.setCacheStrategy(cacheStrategy);
            list.add(node);

            for (int j = 0; j < 2; j++) {
                ECSNode replica = (ECSNode) avaRepica.pollFirst();
                replica.setCachesize(cacheSize);
                replica.setCacheStrategy(cacheStrategy);
                replica.setStartingHashValue(node.getStartingHashValue());
                list.add(replica);
            }
        }
        return list;
    }

    public void startAllNodes() {
        zkWatch.writeData(ROOT_PATH, MetaData.MetaToJson(meta));
    }

    public void sendMeta(IECSNode node) {
        zkWatch.writeData(NODE_PATH_SUFFIX + node.getNodeName(), MetaData.MetaToJson(meta));
    }


    public void initServers(TreeSet<IECSNode> list) {

        zkWatch.setSemaphore(list.size() * 2);

        for (Iterator<IECSNode> iterator = list.iterator(); iterator.hasNext(); ) {
            ECSNode node = (ECSNode) iterator.next();

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

    public void addMeta(TreeSet<IECSNode> list) {
        meta.getServerRepo().addAll(list);
        meta = new MetaData(meta.getServerRepo());
    }

    public boolean removeNodes(Collection<String> nodeNames) {

        for (String nodeName : nodeNames) {
            IECSNode coordinator = meta.getCoordinator(nodeName);
            TreeSet<IECSNode> replica = meta.getReplica(nodeName);

            avaServer.add(meta.removeNode(coordinator.getNodeName()));
            for (IECSNode node : replica)
                avaRepica.add(meta.removeNode(node.getNodeName()));

        }

        return true;
    }


    public boolean awaitNodes(int timeout) {
        return zkWatch.awaitNodes(timeout);
    }

    public boolean stop() {
        return zkWatch.writeData(ROOT_PATH, "");
    }

    public boolean shutdown() {
        return zkWatch.deleteAllNodes(meta.getServerRepo());
    }

    public TreeSet<IECSNode> getNodes() {
        return meta.getServerRepo();
    }

    public void notifyNodes(Collection<String> list) {
        for (String name : list) {
            zkWatch.writeData(NODE_PATH_SUFFIX + name, MetaData.MetaToJson(meta));
        }
    }

    public void notifySuccessor(TreeSet<IECSNode> serversTaken) {

        if (serversTaken.size()  == meta.getServerRepo().size())
            return;

        HashSet<String> list = new HashSet<>();

        for (IECSNode node : serversTaken) {

            if (!((ECSNode) node).getNodeType())
                continue;

            String successor = meta.getSuccessor(node.getNodeName());

            while (serversTaken.contains(meta.getNode(successor)))
                successor = meta.getSuccessor(successor);

            System.out.println(successor);

            list.add(successor);

        }
        notifyNodes(list);
    }
}

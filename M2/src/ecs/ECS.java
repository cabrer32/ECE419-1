package ecs;

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
<<<<<<< Updated upstream
    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar /Users/wuqili/Desktop/ECE419/M2/m2-server.jar %s %s %s %s %s %s &";
    //  private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar /Users/pannnnn/UTcourses/ECE419/Milestones/ece419/M2/m2-server.jar %s %s %s %s %s %s &";
=======
//    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar /Users/wuqili/Desktop/ECE419/M2/m2-server.jar %s %s %s %s %s %s &";
    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar /Users/pannnnn/UTcourses/ECE419/Milestones/ece419/M2/m2-server.jar %s %s %s %s %s %s &";
>>>>>>> Stashed changes

    private ECSWatcher zkWatch;

    private MetaData meta;
    private TreeSet<IECSNode> avaServer = new TreeSet<>();


    private static final String LOCAL_HOST = "127.0.0.1";
    private static final String CONNECTION_ADDR_HOST = "127.0.0.1";
    private static final String CONNECTION_ADDR_PORT = "2181";
    private static final String ROOT_PATH = "/ecs";
    private static final String NODE_PATH_SUFFIX = "/ecs/";

    /**
     * Initialize
     **/
    public ECS(String zkHostname, int zkPort, String configFileName) {
        loadFile(configFileName);

        Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);

        zkWatch = new ECSWatcher();
        zkWatch.init(zkHostname, zkPort);

        meta = new MetaData(new TreeSet<>());
    }

    private void loadFile(String configFileName) {
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
                node = new ECSNode(name, host, port, startingHash);
                avaServer.add(node);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Error! Unable to open the file!");
            e.printStackTrace();
            System.exit(1);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        }
    }



    /**
     *
     * Following function will take command from ecs client
     *
     * */

    public void initServers(TreeSet<IECSNode> list) {

        zkWatch.setSemaphore(list.size(), null);

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

    public void addServers(TreeSet<IECSNode> servers) {

        for (IECSNode node : servers)
            meta.addNode(node);

        meta.setHashRange();


        Set<String> list = new HashSet<>();

        for (IECSNode node : servers) {

            String successor = meta.getSuccessor(node.getNodeName());

            if (!servers.contains(meta.getNode(successor)))
                list.add(successor);

            String predecessor = meta.getSuccessor(node.getNodeName());

            if (!servers.contains(meta.getNode(predecessor)))
                list.add(predecessor);
        }

        notifyNodes(list);
    }


    public boolean removeServers(Collection<String> nodeNames, boolean reuse) {

        for (String name : nodeNames) {
            IECSNode node = meta.removeNode(name);
            if (reuse)
                avaServer.add(node);
        }

        meta.setHashRange();

        notifyNodes(nodeNames);

        return true;
    }


    public TreeSet<IECSNode> getServers() {
        return meta.getServerRepo();
    }



    public TreeSet<IECSNode> getAvailableServers(int count, String cacheStrategy, int cacheSize) {

        if (avaServer.size() < count) {
            logger.error("Do not have enough servers");
            return null;
        }


        TreeSet<IECSNode> list = new TreeSet<>();

        for (int i = 0; i < count; i++) {
            ECSNode node = (ECSNode) avaServer.pollFirst();
            node.setCachesize(cacheSize);
            node.setCacheStrategy(cacheStrategy);
            list.add(node);
        }

        return list;
    }




    /**
     *
     * Following functions will interact with ECS watcher
     *
     **/



    public void broadcastMeta() {
        zkWatch.writeData(ROOT_PATH, MetaData.MetaToJson(meta));
    }


    private void notifyNodes(Collection<String> list) {

        logger.info("Start rearranging servers.");

        zkWatch.setSemaphore(list.size(), list);

        for (String name : list) {
            zkWatch.writeData(NODE_PATH_SUFFIX + name, MetaData.MetaToJson(meta));
        }


        awaitNodes(100000000);

        logger.info("Finish rearranging new nodes.");
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
}

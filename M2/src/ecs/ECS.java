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
//    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar /m2-server.jar %s %s %s %s %s %s &";
//    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar /nfs/ug/homes-4/w/wuzhensh/m2/ece419/M2/m2-server.jar %s %s %s %s %s %s &";
//    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar /Users/wuqili/Desktop/ECE419/M2/m2-server.jar %s %s %s %s %s %s &";
//    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar /Users/pannnnn/UTcourses/ECE419/Milestones/ece419/M2/m2-server.jar %s %s %s %s %s %s &";
//    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar /Users/wuqili/Desktop/ECE419/M2/m2-server.jar %s %s %s %s %s %s &";
    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar /Users/pannnnn/UTcourses/ECE419/Milestones/ece419/M2/m2-server.jar %s %s %s %s %s %s &";

    private ECSWatcher zkWatch;

    private MetaData meta;
    private TreeSet<IECSNode> avaServer = new TreeSet<>();

    private String zkHostname;
    private int zkPort;

    private static final String ROOT_PATH = "/ecs";

    private HashMap<String, ECSDetector> detectors;

    /**
     * Initialize
     **/
    public ECS(String zkHostname, int zkPort, String configFileName) {
        loadFile(configFileName);

        Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);

        this.zkHostname = zkHostname;
        this.zkPort = zkPort;

        zkWatch = new ECSWatcher();
        zkWatch.init(zkHostname, zkPort);

        meta = new MetaData(new TreeSet<IECSNode>());
        detectors = new HashMap<>();
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
     * Following function will take command from ecs client
     */

    public void initServers(TreeSet<IECSNode> list) {

        zkWatch.setSemaphore(list.size());

        for (Iterator<IECSNode> iterator = list.iterator(); iterator.hasNext(); ) {
            ECSNode node = (ECSNode) iterator.next();

            String script = String.format(SCRIPT_TEXT, node.getNodeHost(), node.getNodeName(), zkHostname,
                    zkPort, node.getNodePort(), node.getCacheStrategy(), node.getCachesize());

            Runtime run = Runtime.getRuntime();
            try {
                logger.info("Running ... " + script);
                run.exec(script);
            } catch (IOException e) {
                logger.error("Failed to execute script!");
            }
        }
    }

    public void updateServerMeta() {
        logger.info("--- Updating server meta ---");

        zkWatch.setSemaphore(meta.getServerRepo().size());

        broadcastMeta("F");

        awaitNodes(100000000);

        logger.info("- Done! -");
    }


    public void updateServerData() {
        logger.info("--- Updating server data ---");

        zkWatch.setSemaphore(meta.getServerRepo().size());

        broadcastMeta("C");

        awaitNodes(100000000);

        logger.info("- Done! -");
    }


    public void updateServerReplica() {
        logger.info("--- Updating server replica ---");

        zkWatch.setSemaphore(meta.getServerRepo().size());

        broadcastMeta("D");

        awaitNodes(100000000);

        logger.info("- Done! -");
    }


    public boolean removeServers(Collection<String> nodeNames, boolean reuse) {

        for (String name : nodeNames) {
            IECSNode node = meta.removeNode(name);
            if (reuse)
                avaServer.add(node);
        }

        meta.setHashRange();

        //remove

        broadcastMeta("E");


        for (String name : nodeNames) {
            zkWatch.deleteNode(ROOT_PATH + "/" + name);
        }


        //update replica

        updateServerReplica();

        updateServerMeta();

        return true;
    }


    public TreeSet<IECSNode> getServers() {
        return meta.getServerRepo();
    }


    public TreeSet<IECSNode> setupNewServers(int count, String cacheStrategy, int cacheSize) {

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
            meta.addNode(node);
            zkWatch.watchNewNode(node.getNodeName());
        }

        meta.setHashRange();

        return list;
    }


    /**
     * Following functions will interact with ECS watcher
     **/


    public void broadcastMeta(String type) {
        zkWatch.writeData(ROOT_PATH, MetaData.MetaToJson(type, meta));
    }


    public boolean awaitNodes(int timeout) {
        return zkWatch.awaitNodes(timeout);
    }

    public boolean start() {

        logger.info("--- Starting servers ---");

        zkWatch.setSemaphore(meta.getServerRepo().size());

        broadcastMeta("A");

        awaitNodes(100000000);

        logger.info("- Done! -");

        return true;
    }

    public boolean stop() {
        broadcastMeta("B");
        return true;
    }

    public boolean shutdown() {
        removeDetectors(meta.getNameList());

        boolean flag = zkWatch.deleteAllNodes(meta.getServerRepo());
        zkWatch.releaseConnection();
        return flag;
    }

    /**
     * Following functions will interact with ECSDetector
     */

    public void addDetectors(Collection<IECSNode> list) {

        for (IECSNode node : list) {
            ECSDetector detector = new ECSDetector(logger, this, node);
            detectors.put(node.getNodeName(), detector);
            new Thread(detector).start();
        }
    }

    public void removeDetectors(Collection<String> list) {
        for (String node : list) {
            ECSDetector detector = detectors.remove(node);
            detector.stop();
        }
    }

    public void handleFailure(IECSNode node) {
        ArrayList<String> list = new ArrayList<>();
        list.add(node.getNodeName());

        removeServers(list, false);
        detectors.remove(node.getNodeName());
    }
}

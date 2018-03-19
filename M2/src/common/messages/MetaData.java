package common.messages;

import ecs.ECSNode;
import ecs.IECSNode;
import org.apache.log4j.Logger;
import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileNotFoundException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class MetaData implements IMetaData {
    private static Logger logger = Logger.getRootLogger();
    private String configFileName;
    private TreeSet<IECSNode> serverRepo = new TreeSet<>();
    private TreeSet<IECSNode> serverRepoTaken = new TreeSet<>();
    private HashMap<IECSNode, Integer> serverRepoMapping = new HashMap<>();

    public MetaData(String configFileName) {
        this.configFileName = configFileName;
    }

    public void loadMetaData(){
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
            this.serverRepo = (TreeSet<IECSNode>) serverRepo.clone();
        } catch (FileNotFoundException e) {
            System.out.println("Error! Unable to open the file!");
            e.printStackTrace();
            System.exit(1);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        }
    }

    private void arrangeECSNodes(int count, String cacheStrategy, int cacheSize) {
        int availableNodes = 0;
        for (Integer i : serverRepoMapping.values()) {
            availableNodes += i;
        }
        if (availableNodes < count) {
            return ;
        } else {
            int i = 0;
            for (IECSNode node : serverRepo) {
                if (serverRepoMapping.get(node) == 1) {
                    ((ECSNode) node).setCacheStrategy(cacheStrategy);
                    ((ECSNode) node).setCachesize(cacheSize);
                    serverRepoTaken.add(node);
                    serverRepoMapping.put(node, 0);
                    i = i + 1;
                }
                if (i == count) {
                    break;
                }
            }
        }
        setHashRange();
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

    public TreeSet<IECSNode> getECSNodes(int count, String cacheStrategy, int cacheSize) {
        arrangeECSNodes(count, cacheStrategy, cacheSize);
        return this.serverRepoTaken;
    }

    public IECSNode removeNodes(Collection<String> nodeNames) {
        return null;
    }

    public IECSNode addNodes(int count, String cacheStrategy, int cacheSize) {
        return null;
    }

    public String toJson(){
        return "";
    }

    @Override
    public IECSNode getPrecessor(String name) {

        return null;
    }

    @Override
    public IECSNode getSuccessor(String name) {
        return null;
    }

    @Override
    public IECSNode getServerByKey(String key, boolean write) {
        return null;
    }

    @Override
    public IECSNode[] getReplica(String name) {
        return new IECSNode[0];
    }

    @Override
    public IECSNode getCoordinator(String name) {
        return null;
    }

}

package common.messages;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import ecs.ECSNode;
import ecs.IECSNode;
import org.apache.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class MetaData implements IMetaData {
    private TreeSet<IECSNode> serverRepo;

    public MetaData(TreeSet<IECSNode> serverRepo) {
        this.serverRepo = serverRepo;
    }

    public TreeSet<IECSNode> getServerRepo() {
        return serverRepo;
    }

    @Override
    public String getPredecessor(String name) {
        Iterator itr = serverRepo.iterator();
        if (serverRepo.size() == 1) {
            return ((IECSNode)itr.next()).getNodeName();
        }
        int idx = 0;
        ECSNode prevNode, curNode;
        while (itr.hasNext()) {
            prevNode = (ECSNode) itr.next();
            if (idx == 0 && prevNode.getNodeName().equals(name)) {
                return serverRepo.last().getNodeName();
            }
            if (itr.hasNext()) {
                curNode = (ECSNode) itr.next();
                if (curNode.getNodeName().equals(name)) {
                    return prevNode.getNodeName();
                }
            }
            idx++;
        }
        return null;
    }

    @Override
    public String getSuccessor(String name) {
        Iterator itr = serverRepo.iterator();
        if (serverRepo.size() == 1) {
            return ((IECSNode) itr.next()).getNodeName();
        }
        int idx = 0;
        ECSNode node;
        while (itr.hasNext()) {
            node = (ECSNode) itr.next();
            if (node.getNodeName().equals(name)){
                if (itr.hasNext()) {
                    return ((ECSNode) itr.next()).getNodeName();
                } else if (idx == serverRepo.size() - 1) {
                    return serverRepo.first().getNodeName();
                } else {
                    break;
                }
            }
            idx++;
        }
        return null;
    }

    @Override
    public IECSNode getServerByKey(String key, boolean write) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
            md.update(key.getBytes());
            byte[] digest = md.digest();
            String keyHashValue = DatatypeConverter.printHexBinary(digest).toUpperCase();
            if(write) {
                for (IECSNode node : serverRepo) {
                    if (((ECSNode)node).contains(keyHashValue)) {
                        return node;
                    }
                }
            } else {
                ArrayList<IECSNode> nodes = new ArrayList<>();
                for (IECSNode node : serverRepo) {
                    if (((ECSNode)node).contains(keyHashValue)) {
                        nodes.add(node);
                    }
                }
                int idx = key.hashCode() % 3;
                return nodes.get(idx);
            }
        } catch (NoSuchAlgorithmException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    @Override
    public ArrayList<String> getReplica(String name) {
        ArrayList<String> nodes = new ArrayList<>();
        String startingHashValue = null;
        for (IECSNode node : serverRepo) {
            if (node.getNodeName().equals(name) && ((ECSNode) node).getNodeType()) {
                startingHashValue = ((ECSNode) node).getStartingHashValue();
            }
        }
        if (startingHashValue != null) {
            for (IECSNode node : serverRepo) {
                if (((ECSNode) node).getStartingHashValue().equals(startingHashValue) && !((ECSNode) node).getNodeType()) {
                    nodes.add(node.getNodeName());
                }
            }
        }
        return nodes;
    }

    @Override
    public String getCoordinator(String name) {
        String CoordinatorName = null;
        String startingHashValue = null;
        for (IECSNode node : serverRepo) {
            if (node.getNodeName().equals(name) && !((ECSNode) node).getNodeType()) {
                startingHashValue = ((ECSNode) node).getStartingHashValue();
            }
        }
        if (startingHashValue != null) {
            for (IECSNode node : serverRepo) {
                if (((ECSNode) node).getStartingHashValue().equals(startingHashValue) && ((ECSNode) node).getNodeType()) {
                    CoordinatorName = node.getNodeName();
                }
            }
        }
        return CoordinatorName;
    }

    @Override
    public boolean hasServer(String name) {
        for (IECSNode node : serverRepo) {
            if (node.getNodeName().equals(name)){
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isCoordinator(String name) {
        for (IECSNode node : serverRepo) {
            if (node.getNodeName().equals(name) && ((ECSNode) node).getNodeType()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String[] getHashRange(String name) {
        String[] hashRange = {};
        for (IECSNode node : serverRepo) {
            if (node.getNodeName().equals(name)) {
                hashRange = node.getNodeHashRange();
            }
        }
        return hashRange;
    }

    @Override
    public ArrayList<IECSNode> getServerBetween(String predecessor, String successor) {
        ArrayList<IECSNode> nodes = new ArrayList<>();
        if (predecessor.equals(successor)) {
            return nodes;
        }
        ECSNode node;
        Iterator itr = serverRepo.iterator();
        if (getHashRange(predecessor)[0].compareTo(getHashRange(successor)[0]) < 0) {
            while (itr.hasNext()) {
                node = (ECSNode) itr.next();
                if (node.getNodeName().equals(predecessor)) {
                    break;
                }
            }
            while (itr.hasNext()) {
                node = (ECSNode) itr.next();
                if (!node.getNodeName().equals(successor)) {
                    nodes.add(node);
                } else {
                    break;
                }
            }
            return nodes;
        } else {
            while (itr.hasNext()) {
                node = (ECSNode) itr.next();
                if (!node.getNodeName().equals(predecessor)) {
                    nodes.add(node);
                }
            }
            while (itr.hasNext()) {
                if(((ECSNode) itr.next()).getNodeName().equals(successor)) {
                    break;
                }
            }
            while (itr.hasNext()) {
                node = (ECSNode) itr.next();
                nodes.add(node);
            }
            return nodes;
        }
    }


    public static String MetaToJson (MetaData meta) {
        try {
            return new Gson().toJson(meta, MetaData.class);
        } catch (JsonSyntaxException e) {
            System.out.println("Invalid Message syntax " + e.getMessage());
        }
        return null;
    }

    public static MetaData JsonToMeta (String meta) {
        try {
            return new Gson().fromJson(meta, MetaData.class);
        } catch (JsonSyntaxException e) {
            System.out.println("Invalid Message syntax " + e.getMessage());
        }
        return null;
    }
}
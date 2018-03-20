package common.messages;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import ecs.ECS;
import ecs.ECSNode;
import ecs.IECSNode;

import javax.xml.bind.DatatypeConverter;
import java.lang.reflect.Type;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class MetaData implements IMetaData {
    private TreeSet<IECSNode> serverRepo;


    public MetaData(TreeSet<IECSNode> serverRepo) {
        this.serverRepo = serverRepo;
        setHashRange();
    }

    private void setHashRange() {
        TreeSet<IECSNode> coordinators = getAllCoordinator();
        if (coordinators.size() == 0)
            return;
        ((ECSNode) coordinators.last()).setEndingHashValue(((ECSNode) coordinators.first()).getStartingHashValue());
        Iterator itr = coordinators.iterator();
        TreeSet<IECSNode> replicas;
        ECSNode currentNode, nextNode;
        if (itr.hasNext()) {
            currentNode = (ECSNode) itr.next();
            replicas = getReplica(currentNode.getNodeName());
            while (itr.hasNext()) {
                nextNode = (ECSNode) itr.next();
                currentNode.setEndingHashValue(nextNode.getStartingHashValue());
                for (IECSNode replica : replicas) {
                    ((ECSNode)replica).setEndingHashValue(nextNode.getStartingHashValue());
                }
                currentNode = nextNode;
            }
        }
    }

    public TreeSet<IECSNode> getServerRepo() {
        return serverRepo;
    }


    @Override
    public String getPredecessor(String name) {
        TreeSet<IECSNode> nodes = getAllCoordinator();
        Iterator itr = nodes.iterator();
        if (nodes.size() == 1) {
            return ((IECSNode) itr.next()).getNodeName();
        }
        int idx = 0;
        ECSNode prevNode, curNode;
        while (itr.hasNext()) {
            prevNode = (ECSNode) itr.next();
            if (idx == 0 && prevNode.getNodeName().equals(name)) {
                return nodes.last().getNodeName();
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
        TreeSet<IECSNode> nodes = getAllCoordinator();
        Iterator itr = nodes.iterator();
        if (nodes.size() == 1) {
            return ((IECSNode) itr.next()).getNodeName();
        }
        int idx = 0;
        ECSNode node;
        while (itr.hasNext()) {
            node = (ECSNode) itr.next();
            if (node.getNodeName().equals(name)) {
                if (itr.hasNext()) {
                    return ((ECSNode)itr.next()).getNodeName();
                } else if (idx == nodes.size() - 1) {
                    return nodes.first().getNodeName();
                } else {
                    break;
                }
            }
            idx++;
        }
        return null;
    }

    public TreeSet<IECSNode> getAllCoordinator(){
        TreeSet<IECSNode> coordinators = new TreeSet<>();
        Iterator itr = serverRepo.iterator();
        ECSNode node;
        while (itr.hasNext()) {
            node = (ECSNode) itr.next();
            if (node.getNodeType()) {
                coordinators.add(node);
            }
        }
        return coordinators;
    }

    @Override
    public IECSNode getServerByKey(String key, boolean write) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
            md.update(key.getBytes());
            byte[] digest = md.digest();
            String keyHashValue = DatatypeConverter.printHexBinary(digest).toUpperCase();
            if (write) {
                for (IECSNode node : serverRepo) {
                    if (((ECSNode) node).contains(keyHashValue)) {
                        return node;
                    }
                }
            } else {
                ArrayList<IECSNode> nodes = new ArrayList<>();
                for (IECSNode node : serverRepo) {
                    if (((ECSNode) node).contains(keyHashValue)) {
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
    public TreeSet<IECSNode> getReplica(String name) {
        TreeSet<IECSNode> nodes = new TreeSet<>();
        String startingHashValue = null;
        for (IECSNode node : serverRepo) {
            if (node.getNodeName().equals(name) && ((ECSNode) node).getNodeType()) {
                startingHashValue = ((ECSNode) node).getStartingHashValue();
            }
        }
        if (startingHashValue != null) {
            for (IECSNode node : serverRepo) {
                if (((ECSNode) node).getStartingHashValue().equals(startingHashValue) && !((ECSNode) node).getNodeType()) {
                    nodes.add(node);
                }
            }
        }
        return nodes;
    }

    @Override
    public IECSNode getCoordinator(String name) {
        IECSNode coordinator = null;
        String startingHashValue = null;
        for (IECSNode node : serverRepo) {
            if (node.getNodeName().equals(name) && !((ECSNode) node).getNodeType()) {
                startingHashValue = ((ECSNode) node).getStartingHashValue();
            }
        }
        if (startingHashValue != null) {
            for (IECSNode node : serverRepo) {
                if (((ECSNode) node).getStartingHashValue().equals(startingHashValue) && ((ECSNode) node).getNodeType()) {
                    coordinator = node;
                }
            }
        }
        return coordinator;
    }

    @Override
    public boolean hasServer(String name) {
        for (IECSNode node : serverRepo) {
            if (node.getNodeName().equals(name)) {
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
                if (((ECSNode) itr.next()).getNodeName().equals(successor)) {
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

    @Override
    public IECSNode getNode(String name) {
        for (IECSNode node : serverRepo) {
            if (node.getNodeName().equals(name)) {
                return node;
            }
        }
        return null;
    }

    @Override
    public IECSNode removeNode(String name) {
        Iterator iter = serverRepo.iterator();
        IECSNode node;
        while (iter.hasNext()) {
            node = (IECSNode) iter.next();
            if (node.getNodeName().equals(name)) {
                iter.remove();
                return node;
            }
        }
        return null;
    }


    public static String MetaToJson(MetaData meta) {
        try {
            Type metaType = new TypeToken<MetaData>() {
            }.getType();

            return new Gson().toJson(meta, metaType);
        } catch (JsonSyntaxException e) {
            System.out.println("Invalid Message syntax " + e.getMessage());
        }
        return null;
    }

    public static MetaData JsonToMeta(String meta) {
        try {
            Type metaType = new TypeToken<MetaData>() {
            }.getType();

            return new Gson().fromJson(meta, metaType);
        } catch (JsonSyntaxException e) {
            System.out.println("Invalid Message syntax " + e.getMessage());
        }
        return null;
    }
}
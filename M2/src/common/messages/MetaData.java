package common.messages;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import ecs.ECSNode;
import ecs.IECSNode;
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
        for (IECSNode node : serverRepo) {
            if (((ECSNode)node).contains(key)) {
                return node;
            }
        }
        return null;
    }

    @Override
    public IECSNode[] getReplica(String name) {
        return new IECSNode[0];
    }

    @Override
    public String getCoordinator(String name) {
        return null;
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
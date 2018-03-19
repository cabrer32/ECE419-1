package common.messages;


import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import ecs.ECSNode;
import ecs.IECSNode;

import java.lang.reflect.Type;
import java.util.*;

public class MetaData implements IMetaData {

    private TreeSet<IECSNode> serverSet = null;


    public MetaData(TreeSet<IECSNode> serverSet) {
        this.serverSet = serverSet;
    }


    @Override
    public String getPredecessor(String name) {
        Iterator itr = serverSet.iterator();
        ECSNode prevNode, curNode;
        while (itr.hasNext()) {
            prevNode = (ECSNode) itr.next();
            if (itr.hasNext()) {
                curNode = (ECSNode) itr.next();
                if (curNode.getNodeName().equals(name)) {
                    return prevNode.getNodeName();
                }
            }
        }
        return null;
    }

    @Override
    public String getSuccessor(String name) {
        Iterator itr = serverSet.iterator();
        ECSNode node;
        while (itr.hasNext()) {
            node = (ECSNode) itr.next();
            if (node.getNodeName().equals(name)){
                if (itr.hasNext()) {
                    return ((ECSNode) itr.next()).getNodeName();
                } else {
                    break;
                }
            }
        }
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
    public String getCoordinator(String name) {
        return null;
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
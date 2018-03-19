package common.messages;


import ecs.ECSNode;
import ecs.IECSNode;

import java.util.*;

public class MetaData implements IMetaData {

    private TreeSet<IECSNode> serverSet = null;


    public MetaData(TreeSet<IECSNode> serverSet) {
        this.serverSet = serverSet
    }


    @Override
    public IECSNode getPredecessor(String name) {
        Iterator itr = serverRepoTaken.iterator();
        ECSNode prevNode, curNode;
        while (itr.hasNext()) {
            prevNode = (ECSNode) itr.next();
            if (itr.hasNext()) {
                curNode = (ECSNode) itr.next();
                if (curNode.getNodeName().equals(name)) {
                    return prevNode;
                }
            }
        }
        return null;
    }

    @Override
    public IECSNode getSuccessor(String name) {
        Iterator itr = serverRepoTaken.iterator();
        ECSNode node;
        while (itr.hasNext()) {
            node = (ECSNode) itr.next();
            if (node.getNodeName().equals(name)){
                if (itr.hasNext()) {
                    return (ECSNode) itr.next();
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
    public IECSNode getCoordinator(String name) {
        return null;
    }

}

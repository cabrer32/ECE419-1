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
        ECSNode preveNode, nextNode;
        while (itr.hasNext()) {
            preveNode = (ECSNode) itr.next();
            if (itr.hasNext()) {
                nextNode = (ECSNode) itr.next();
                if (nextNode.getNodeName().equals(name)) {
                    return preveNode;
                }
            }
        }
        return null;
    }

    @Override
    public IECSNode getSuccessor(String name) {
        Iterator itr = serverRepoTaken.iterator();
        ECSNode currentNode, nextNode;
        while (itr.hasNext()) {
            currentNode = (ECSNode) itr.next();
            if (itr.hasNext()) {
                nextNode = (ECSNode) itr.next();
                if (nextNode.getNodeName().equals(name)) {
                    return currentNode;
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

package common.messages;

import ecs.IECSNode;

import java.util.ArrayList;
import java.util.TreeSet;

public interface IMetaData {
    public String getPredecessor(String name);
    public String getSuccessor(String name);
    public IECSNode getServerByKey(String key, boolean write);
    public TreeSet<IECSNode> getReplica(String name);
    public IECSNode getCoordinator(String name);
    public boolean hasServer(String name);
    public boolean isCoordinator(String name);
    public String[] getHashRange(String name);
    public ArrayList<IECSNode> getServerBetween(String precessor, String name);

    public IECSNode getNode(String name);

    public IECSNode removeNode(String name);
}

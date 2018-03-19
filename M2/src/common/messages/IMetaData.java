package common.messages;

import ecs.IECSNode;

public interface IMetaData {
    public String getPredecessor(String name);
    public String getSuccessor(String name);
    public IECSNode getServerByKey(String key, boolean write);
    public IECSNode[] getReplica(String name);
    public IECSNode getCoordinator(String name);
    public boolean hasServer(String name);
}

package common.messages;

import ecs.IECSNode;

public interface IMetaData {
    public String getPredecessor(String name);
    public String getSuccessor(String name);
    public IECSNode getServerByKey(String key, boolean write);
    public IECSNode[] getReplica(String name);
    public String getCoordinator(String name);
    public boolean hasServer(String name);


    public boolean isCoordinator(String name);

    public String[] getHashRange(String name);

    public IECSNode[] getServerBetween(String precessor, String name);

}

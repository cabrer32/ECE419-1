package common.messages;

import ecs.IECSNode;

public interface IMetaData {

    public IECSNode getPrecessor(String name);
    public IECSNode getSuccessor(String name);
    public IECSNode getServerByKey(String key, boolean write);
    public IECSNode[] getReplica(String name);
    public IECSNode getCoordinator(String name);
}

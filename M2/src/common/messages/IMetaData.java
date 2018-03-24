package common.messages;

import ecs.IECSNode;

import java.util.ArrayList;
import java.util.TreeSet;

public interface IMetaData {
    String getPredecessor(String name);
    String getSuccessor(String name);
    String[] getHashRange(String name);
    ArrayList<IECSNode> getServerBetween(String predecessor, String name);
    IECSNode getNode(String name);
    ArrayList<String> getReplica(String name);
    void addNode(IECSNode node);
    IECSNode removeNode(String name);
}

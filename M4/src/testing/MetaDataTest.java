package testing;

import common.messages.IMetaData;
import common.messages.MetaData;
import ecs.ECS;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeSet;

public class MetaDataTest extends TestCase {

    public IMetaData metaData = null;
    public String[] serverList = {"server8", "server6", "server1", "server7", "server4", "server3", "server5", "server2"};

    @Before
    public void setUp() {


        TreeSet<IECSNode> list = new TreeSet<>();


        for(int i = 0 ; i < serverList.length; i++) {
            list.add(new ECSNode(serverList[i], "127.0.0.1", 50007, Integer.toString(i)));
        }
        metaData = new MetaData(list);

    }

    @After
    public void tearDown() {
        metaData = null;
    }

    @Test
    public void testGetPredecessor() {
        String predecessor;
        for (int i = 1; i < serverList.length; i++) {
            predecessor = metaData.getPredecessor(serverList[i]);
            assertEquals("getPredecessor() failed!", serverList[i-1], predecessor);
        }
    }

    @Test
    public void testGetSuccessor() {
        String successor;
        for (int i = serverList.length - 1; i > 0 ; i--) {
            successor = metaData.getPredecessor(serverList[i]);
            assertEquals("getPredecessor() failed!", serverList[i-1], successor);
        }
    }

    @Test
    public void testGetServerBetween() {
        getServerBetweenWrapper("server8", "server4", new String[]{"server6", "server1", "server7"});
        metaData.removeNode("server1");
        getServerBetweenWrapper("server8", "server4", new String[]{"server6", "server7"});
        getServerBetweenWrapper("server5", "server6", new String[]{"server2", "server8"});
    }

    private void getServerBetweenWrapper(String predecessor, String name, String[] expectedArray) {
        ArrayList<String > expected = new ArrayList<>(Arrays.asList(expectedArray));
        Collections.sort(expected);
        ArrayList<IECSNode> nodesList = metaData.getServerBetween(predecessor, name);
        ArrayList<String> result = new ArrayList<>();
        for (IECSNode node : nodesList) {
            result.add(node.getNodeName());
        }
        Collections.sort(result);
        assertEquals("getServersBetween() failed!", expected, result);
    }

    @Test
    public void testGetReplica() {
        getReplicaWrapper("server8", new String[]{"server6", "server1"});
        metaData.removeNode("server1");
        getReplicaWrapper("server8", new String[]{"server6", "server7"});
        getReplicaWrapper("server2", new String[]{"server8", "server6"});
    }

    private void getReplicaWrapper(String name, String[] expectedArray) {
        ArrayList<String > expected = new ArrayList<>(Arrays.asList(expectedArray));
        Collections.sort(expected);
        ArrayList<String> result = metaData.getReplica(name);
        Collections.sort(result);
        assertEquals("getReplica() failed!", expected, result);
    }

    @Test
    public void testAddNode() {
        IECSNode node = new ECSNode("testserver1", "testhost1", 0, "a");
        metaData.addNode(node);
        IECSNode result = metaData.getNode("testserver1");
        assertEquals("addnode() failed!", node, result);
    }

    @Test
    public void testRemoveNode() {
        metaData.removeNode("server8");
        IECSNode result = metaData.getNode("server8");
        assertNull("removeNode() failed!", result);
    }
}

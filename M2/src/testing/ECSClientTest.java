package testing;

import app_kvECS.ECSClient;
import ecs.IECSNode;
import junit.framework.TestCase;
import org.junit.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class ECSClientTest extends TestCase {

    @Test
    public void testECS() {

        ECSClient ecsClient = new ECSClient("127.0.0.1",2181,"ecs.config");

        // for initialization and test result purpose
        int[] addSizes = {3, 2};
        String cacheStrategy = "None";
        int cacheSizes = 100;
        String[] nodeNames = {"server8", "server6", "server1", "server7", "server4"};

        /**
         * addnodes() before start()
         */
        Collection<IECSNode> firstAdd = ecsClient.addNodes(addSizes[0], cacheStrategy, cacheSizes);
        assertNotNull("addNodes() failed.", firstAdd);
        assertEquals("addNodes() failed.", firstAdd.size(), addSizes[0]);

        /**
         * start()
         */
        assertTrue("start() failed.", ecsClient.start());

        /**
         * addnodes() after start()
         */
        Collection<IECSNode> secondAdd = ecsClient.addNodes(addSizes[1], cacheStrategy, cacheSizes);
        assertNotNull("addNodes() failed.", secondAdd);
        assertEquals("addNodes() failed.", secondAdd.size(), addSizes[1]);

        /**
         * getNodes()
         */
        Map<String, IECSNode> nodes = ecsClient.getNodes();
        assertNotNull("getNodes() failed.", nodes);
        Set<String> resNodeNames = nodes.keySet();
        for (String nodeName : nodeNames) {
            assertTrue("getNodes() failed.", resNodeNames.contains(nodeName));
        }

        /**
         * getNodeByKey()
         */
        IECSNode node = ecsClient.getNodeByKey("25000000000000000000000000000000");
        assertNotNull("getNodeByKey() failed.", node);

        assertTrue(node.getNodeName().equals(nodeNames[1]));


        node = ecsClient.getNodeByKey("30000000000000000000000000000000");
        assertNotNull("getNodeByKey() failed.", node);

        assertTrue(node.getNodeName().equals(nodeNames[2]));
        node = ecsClient.getNodeByKey("40000000000000000000000000000000");
        assertNotNull("getNodeByKey() failed.", node);
        assertTrue(node.getNodeName().equals(nodeNames[3]));

        /**
         * removeNodes()
         */
        Collection<String> removeNodes = new ArrayList<>();
        removeNodes.add(nodeNames[0]);
        assertTrue("removeNodes() failed.", ecsClient.removeNodes(removeNodes));

        /**
         * shutdown()
         */
        assertTrue("shutdown() failed.", ecsClient.shutdown());

    }
}

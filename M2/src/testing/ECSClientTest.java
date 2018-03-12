package testing;

import app_kvECS.ECSClient;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import org.junit.Test;

import java.rmi.server.ExportException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ECSClientTest extends TestCase {


    private ECSClient ecsClient = null;

    @Override
    public void setUp() {

        ecsClient = new ECSClient("ecs.config");
    }

    @Override
    protected void tearDown() {
        ecsClient.shutdown();
        System.out.println("Server has been teared down");
    }

    @Test
    public void testECS() {
        try {
            boolean s = false;
            /** test add nodes */
            Collection<IECSNode> nodes = ecsClient.addNodes(1, "LFU", 50);

            if (nodes == null || nodes.size() != 1)
                assertTrue(false);


            /** test remove nodes */
            Collection<String> removenodes = new ArrayList<>();

            removenodes.add("server8");

            s = ecsClient.removeNodes(removenodes);
            assertTrue(s);

            /** test get nodes */
            Map<String, IECSNode> maps = ecsClient.getNodes();

            assertTrue(maps != null);


            /** test get nodes */
            IECSNode node = ecsClient.getNodeByKey("00000000000000000000000000000000");

            assertTrue(node == null);


        } catch (Exception e) {
            System.out.println("Error happening " + e);
            e.printStackTrace();
        }
    }
}

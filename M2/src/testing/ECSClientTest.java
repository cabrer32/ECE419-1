package testing;

import app_kvECS.ECSClient;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import org.junit.Test;

import java.rmi.server.ExportException;
import java.util.ArrayList;
import java.util.Collection;

public class ECSClientTest extends TestCase {


    private ECSClient ecsClient = null;

    @Override
    public void setUp() {


    }

    @Override
    protected void tearDown() {
        ecsClient.shutdown();
        System.out.println("Server has been teared down");
    }


    @Test
    public void testAddNodes() {
        Collection<IECSNode> nodes = ecsClient.addNodes(4, "LFU", 50);

        if(nodes == null || nodes.size() != 4)
            assertTrue(false);
    }

    @Test
    public void testWaitNodes() {
        try {


            boolean s = ecsClient.awaitNodes(3, 5000);
            assertTrue(s);
        } catch (Exception e) {
            assertTrue(false);
            System.out.println("cannot wait nodes");
        }
    }

    @Test
    public void testRemoveNodes() {
        try {


            Collection<IECSNode> nodes = ecsClient.addNodes(4, "LFU", 50);

            if(nodes == null || nodes.size() != 4)
                assertTrue(false);


            Collection<String> removenodes = new ArrayList<>();

            removenodes.add("server8");

            boolean s = ecsClient.removeNodes(removenodes);
            assertTrue(s);
        } catch (Exception e) {
            System.out.println("cannot wait nodes " + e);
        }
    }


}

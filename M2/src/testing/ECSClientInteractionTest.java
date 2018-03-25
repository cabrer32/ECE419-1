package testing;

import app_kvECS.ECSClient;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.*;
import ecs.IECSNode;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class ECSClientInteractionTest extends TestCase {

    private ECSClient ecsClient = null;
    private KVStore kvClient = null;

    @Before
    public void setUp() throws  Exception{
        ecsClient = new ECSClient("127.0.0.1",2181,"ecs.config");
        ecsClient.addNodes(3, "None", 100);
        ecsClient.start();
        kvClient = new KVStore("localhost", 50000);
        kvClient.connect();
    }

    @After
    public void tearDown() {
        kvClient.disconnect();
        ecsClient.shutdown();
    }

    @Test
    public void testDataTransfer() {
        Map<String, IECSNode> originalNodesMap = ecsClient.getNodes();
        try {
            for (int i = 1; i <= 1000; i++) {
                kvClient.put("Replication-" + Integer.toString(i), Integer.toString(i));
            }
        } catch (Exception e) {
            assertTrue("kvClient put key failed!", false);
        }

        Collection<IECSNode> addedNodes =  ecsClient.addNodes(3, "FIFO", 100);

        assertTrue("failed to remove nodes!", ecsClient.removeNodes(originalNodesMap.keySet()));

        KVMessage kvMessage;

        try {
            for (int i = 1; i <= 1000; i++) {
                kvMessage  = kvClient.get("DataTransfer-" + Integer.toString(i));
                assertTrue(kvMessage.getStatus() == StatusType.GET_SUCCESS);
            }
        } catch (Exception e) {
            assertTrue("kvClient get key failed!", false);
        }
    }


    @Test
    public void testKVServerCrashed() {
        Map<String, IECSNode> originalNodesMap = ecsClient.getNodes();
        try {
            for (int i = 1; i <= 1000; i++) {
                kvClient.put("Replication-" + Integer.toString(i), Integer.toString(i));
            }
        } catch (Exception e) {
            assertTrue("kvClient put key failed!", false);
        }

        // add one node to maintain a minimum size of 3 servers
        Collection<IECSNode> addedNodes =  ecsClient.addNodes(1, "FIFO", 100);

        // crash the server remotely
        IECSNode node = originalNodesMap.values().iterator().next();
        String SCRIPT_TEXT = "ssh -n %s nohup lsof -ti :%s | xargs kill -9 &";
        String script = String.format(SCRIPT_TEXT, node.getNodeHost(), node.getNodePort());

        Runtime run = Runtime.getRuntime();
        try {
            run.exec(script);
        } catch (IOException e) {
            assertTrue("Failed to crash server remotely!", false);
        }


        KVMessage kvMessage;

        try {
            for (int i = 1; i <= 1000; i++) {
                kvMessage  = kvClient.get("Replication-" + Integer.toString(i));
                assertTrue(kvMessage.getStatus() == StatusType.GET_SUCCESS);
            }
        } catch (Exception e) {
            assertTrue("kvClient get key failed!", false);
        }

    }
}

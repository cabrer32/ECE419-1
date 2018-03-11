package testing;

import app_kvECS.ECSClient;
import junit.framework.TestCase;
import org.junit.Test;

import java.rmi.server.ExportException;

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
    public void testAddNodes() {
//        ecsClient.addNodes(1, "FIFO", 100);
//        ecsClient.addNodes(1, "LRU", 70);
        ecsClient.addNodes(3, "LFU", 50);

    }

//    @Test
//    public void testWaitNodes() {
//        try {
//
//            boolean s = ecsClient.awaitNodes(3, 5000);
//            assertTrue(s);
//        } catch (Exception e) {
//            assertTrue(false);
//            System.out.println("cannot wait nodes");
//        }
//    }


}

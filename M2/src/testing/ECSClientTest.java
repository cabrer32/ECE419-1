package testing;

import app_kvECS.ECSClient;
import junit.framework.TestCase;
import org.junit.Test;

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
        ecsClient.addNodes(1,"FIFO",100);

    }



}

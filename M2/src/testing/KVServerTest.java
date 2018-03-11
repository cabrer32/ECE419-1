package testing;

import app_kvServer.KVServer;
import common.module.ServerThread;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Test;


public class KVServerTest extends TestCase {

    private KVServer kvServer = null;
    private ServerThread thread = null;

    @Override
    protected void tearDown() {
        thread.interrupt();
        System.out.println("Server has been teared down");
    }


    @Test
    public void testConstructor() {
        try {

            kvServer = new KVServer("testserver1", "127.0.0.1", 2181);
            kvServer.initKVServer(40000, 100, "FIFO");

            thread = new ServerThread(kvServer);
            thread.start();


        } catch (Exception e) {
            assertTrue(false);
            System.out.println("Error happends while initialize server");
        }
    }

}

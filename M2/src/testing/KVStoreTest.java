package testing;

import app_kvServer.KVServer;
import client.KVStore;
import common.module.ServerThread;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.junit.Test;

public class KVStoreTest extends TestCase {

    private KVServer kvServer = null;
    private ServerThread thread = null;

    private KVStore kvClient;

    @Override
    public void setUp() {

        try {

            kvServer = new KVServer("testserver2", "127.0.0.1", 2181);
            kvServer.initKVServer(40001, 100, "FIFO");

            thread = new ServerThread(kvServer);
            thread.start();

        } catch (Exception e) {
            assertTrue(false);
            System.out.println("Cannot initialize Server");
        }
    }

    @Override
    protected void tearDown() {
        thread.interrupt();
        kvServer.close();

        System.out.println("Server has been teared down");
    }

    @Test
    public void testKVStoreConnection() {
        try {

            kvClient = new KVStore("127.0.0.1", 40001);
            kvClient.connect();

        } catch (Exception e) {
            assertTrue(false);
            System.out.println("Error happend while connecting server " + e);
        }
    }


}

package testing;

import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.KVServer;
import common.module.ServerThread;
import junit.framework.TestCase;
import org.junit.*;


public class KVServerTest extends TestCase {

    private KVServer kvServer = null;
    ServerThread thread =  null;

    private static final String KVSERVER_NAME = "testserver1";
    private static final int KVSERVER_PORT = 40000;
    private static final int CACHE_SIZE = 5;
    private static final String CACHE_STRATEGY = "FIFO";


    @BeforeClass
    public void setUp() {
        kvServer = new KVServer(KVSERVER_NAME, "", 0);
        kvServer.initKVServer(KVSERVER_PORT, CACHE_SIZE, CACHE_STRATEGY);
        ServerThread thread = new ServerThread(kvServer);
        thread.start();
    }

    @AfterClass
    public void tearDown() {
        thread.interrupt();
        kvServer.close();
    }

    @Test
    public void testConstructor() {
        assertNull("KVServer Constructor Failed!", kvServer);
    }

    @Test
    public void testGetPort() {
        int port = kvServer.getPort();
        assertEquals("getPort() failed.", KVSERVER_PORT, port);
    }

    @Test
    public void testGetHostname() {
        String hostName = kvServer.getHostname();
        assertEquals("getHostName() failed.", KVSERVER_NAME, hostName);
    }

    @Test
    public void testGetCacheStrategy() {
        CacheStrategy cacheStrategy = kvServer.getCacheStrategy();
        assertEquals("getCacheStrategy() failed.", CacheStrategy.valueOf(CACHE_STRATEGY),  cacheStrategy);
    }

    @Test
    public void testGetCacheSize() {
        int cacheSize = kvServer.getCacheSize();
        assertEquals("getCacheSize() failed.", CACHE_SIZE,  cacheSize);
    }

    @Test
    public void testInStorage() {
        // Tested in KVCacheTest
    }

    @Test
    public void testInCache() {
        // Tested in KVCacheTest
    }

    @Test
    public void testGetKV() {
        // Tested in KVCacheTest
    }

    @Test
    public void testPutKV() {
        // Tested in KVCacheTest
    }

    @Test
    public void testClearCache() {
        // Tested in KVCacheTest
    }

    @Test
    public void testClearStorage() {
        // Tested in KVCacheTest
    }

    @Test
    public void testRun() {
    }

    @Test
    public void testKill() {
    }

    @Test
    public void testClose() {
    }

    @Test
    public void testStart() {
    }

    @Test
    public void testStop() {
    }

    @Test
    public void testLockWrite() {
    }

    @Test
    public void testUnlockWrite() {
    }

    @Test
    public void testMoveData() {
    }

    @Test
    public void testDB() {
        Exception ex = null;
        try {

            // put
            for (int i = 1; i <= 1000; i++) {
                kvServer.putKV("FIFO-" + Integer.toString(i), Integer.toString(i));
            }

            for (int i = 1; i <= 1000; i++) {
                assertTrue("Did not put correctly" + i, kvServer.inStorage("FIFO-" + Integer.toString(i)));
            }

            // delete
            for (int i = 1; i <= 1000; i++) {
                kvServer.putKV("FIFO-" + Integer.toString(i), null);
            }

            for (int i = 1; i <= 1000; i++) {
                assertFalse("Did not delete " + i, kvServer.inStorage("FIFO-" + Integer.toString(i)));
            }

            kvServer.close();
        } catch (Exception e) {
            ex = e;
            System.out.println("testDelete failed " + e);
        }
        assertNull(ex);
    }
}

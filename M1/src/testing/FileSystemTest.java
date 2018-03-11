package testing;

import app_kvServer.KVServer;
import junit.framework.TestCase;
import org.junit.Test;

public class FileSystemTest extends TestCase {

    @Test
    public void testPut() {
        try {

            KVServer server = new KVServer(30000, 5, "None");


            for (int i = 1; i <= 1000; i++) {
                server.putKV("FIFO-" + Integer.toString(i), Integer.toString(i));
                server.getKV("FIFO-" + Integer.toString(i));
            }


            for (int i = 1; i <= 1000; i++) {
               assertTrue("Did not put correctly" + i, server.inStorage("FIFO-" + Integer.toString(i)));
            }

            server.close();
        } catch (Exception e) {
            assertTrue(false);
            System.out.println("testPut failed " + e);
        }
    }

    @Test
    public void testDelete() {
        try {

            KVServer server = new KVServer(30000, 5, "None");

            for (int i = 1; i <= 1000; i++) {
                server.putKV("FIFO-" + Integer.toString(i), Integer.toString(i));
            }

            for (int i = 1; i <= 1000; i++) {
                server.putKV("FIFO-" + Integer.toString(i), null);
            }

            for (int i = 1; i <= 1000; i++) {
                assertFalse("Did not delete " + i, server.inStorage("FIFO-" + Integer.toString(i)));
            }

            server.close();
        } catch (Exception e) {
            assertTrue(false);
            System.out.println("testDelete failed " + e);
        }
    }
}

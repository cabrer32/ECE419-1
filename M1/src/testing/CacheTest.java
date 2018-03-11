package testing;

import app_kvServer.KVServer;
import common.module.ServerThread;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CacheTest extends TestCase {

    @Test
    public void testFIFO() {
        try {

            KVServer server = new KVServer(30000, 5, "FIFO");


            for (int i = 1; i <= 6; i++) {
                server.putKV("FIFO-" + Integer.toString(i), Integer.toString(i));
                server.getKV("FIFO-" + Integer.toString(i));
            }


            for (int i = 1; i <= 6; i++) {
                if (i != 1) {
                    assertTrue("LRU failed - did not put value properly FIFO-" + i, server.inCache("FIFO-" + Integer.toString(i)));
                    assertTrue("File failed - did not put value properly FIFO-" + i, server.inStorage("FIFO-" + Integer.toString(i)));
                } else {
                    assertFalse("LRU failed - did not put value properly FIFO-" + i, server.inCache("FIFO-" + Integer.toString(i)));
                    assertTrue("File failed - did not put value properly FIFO-" + i, server.inStorage("FIFO-" + Integer.toString(i)));
                }
            }

            server.close();
        } catch (Exception e) {
            assertTrue(false);
            System.out.println("FIFO failed " + e);
        }
    }

    @Test
    public void testLFU() {
        try {
            KVServer server = new KVServer(30000, 5, "LFU");
            for (int i = 1; i <= 4; i++) {
                server.putKV("LFU-" + Integer.toString(i), Integer.toString(i));
                server.getKV("LFU-" + Integer.toString(i));
            }


            //5 should be deleted
            server.putKV("LFU-5", "5");
            server.putKV("LFU-6", "6");


            //test keys from 1 - 6, 5 should not be there.

            for (int i = 1; i <= 6; i++) {
                if (i != 5) {

                    assertTrue("LRU failed - did not put value properly LFU-" + i, server.inCache("LFU-" + Integer.toString(i)));
                    assertTrue("File failed - did not put value properly LFU-" + i, server.inStorage("LFU-" + Integer.toString(i)));
                } else {
                    assertFalse("LRU failed - did not put value properly LFU-" + i, server.inCache("LFU-" + Integer.toString(i)));
                    assertTrue("File failed - did not put value properly LFU-" + i, server.inStorage("LFU-" + Integer.toString(i)));
                }
            }

            server.close();

        } catch (Exception e) {
            assertTrue(false);
            System.out.println("LFU test case " + e);
        }
    }

    @Test
    public void testLRU() {
        try {

            KVServer server = new KVServer(30000, 5, "LRU");

            for (int i = 1; i <= 5; i++) {
                server.putKV("LRU-" + Integer.toString(i), Integer.toString(i));
                server.getKV("LRU-" + Integer.toString(i));
            }

            for (int i = 1; i >= 5; i--) {
                server.getKV("LRU-" + Integer.toString(i));
            }

            server.putKV("LRU-6", "6");

            for (int i = 1; i <= 6; i++) {
                if (i != 1) {
                    assertTrue("LRU failed - did not put value properly LRU-" + i, server.inCache("LRU-" + Integer.toString(i)));
                    assertTrue("File failed - did not put value properly LRU-" + i, server.inStorage("LRU-" + Integer.toString(i)));
                } else {
                    assertFalse("LRU failed - did not put value properly LRU-" + i, server.inCache("LRU-" + Integer.toString(i)));
                    assertTrue("File failed - did not put value properly LRU-" + i, server.inStorage("LRU-" + Integer.toString(i)));
                }
            }

            server.close();
        } catch (Exception e) {
            assertTrue(false);
            System.out.println("LRU failed " + e);
        }
    }


}

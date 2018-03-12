package testing;

import app_kvServer.KVServer;
import client.KVStore;
import common.module.ServerThread;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CacheTest extends TestCase {

    KVServer server = null;
    ServerThread serverThread = null;

    @Override
    protected void tearDown() {
        serverThread.interrupt();
        server.close();

        System.out.println("Server has been teared down");
    }



    @Test
    public void testFIFO() {
        try {

            server = new KVServer("test", "",0);
            server.initKVServer(30002, 5, "FIFO");
            server.clearStorage();
            serverThread = new ServerThread(server);
            serverThread.start();


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
            System.out.println("FIFO failed " + e);
        }
    }

    @Test
    public void testLFU() {
        try {

            server = new KVServer("test", "",0);
            server.initKVServer(30000, 5, "LFU");
            server.clearStorage();
            serverThread = new ServerThread(server);
            serverThread.start();


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
            System.out.println("LFU test case " + e);
        }
    }

    @Test
    public void testLRU() {
        try {

            server = new KVServer("test", "",0);
            server.initKVServer(30001, 5, "LRU");
            server.clearStorage();
            serverThread = new ServerThread(server);
            serverThread.start();


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

            System.out.println("LRU failed " + e);
        }
    }


}
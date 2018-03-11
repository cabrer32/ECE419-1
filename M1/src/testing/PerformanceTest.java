package testing;

import app_kvServer.KVServer;

import org.junit.Test;
import junit.framework.TestCase;

import java.util.Random;


public class PerformanceTest extends TestCase {

    private final int testSize = 1000;

    @Test
    public void testCacheLRU() {
        try {
            System.out.println("With Total (put, get) operations of " + testSize);

            for (int size = 200; size <= testSize; size = size + 200) {
                for (int percentage = 20; percentage < 100; percentage = percentage + 20) {

                    KVServer server = new KVServer(30000, size, "LRU");
                    server.clearStorage();

                    for (int i = 1; i <= testSize; i++) {
                        server.putKV("P-" + Integer.toString(i), Integer.toString(i));
                    }

                    long start = System.currentTimeMillis();



                    for (int i = 1; i <= testSize * ((float) percentage / (float) testSize); i++) {
                        int ran = new Random().nextInt(testSize-1)+1;
                        server.putKV("P-" + Integer.toString(ran), Integer.toString(ran));

                    }

                    for (int i = 1; i <= testSize * (1 - ((float) percentage / (float) testSize)); i++) {
                        int ran = new Random().nextInt(testSize-1)+1;
                        String value = server.getKV("P-" + Integer.toString(ran));
                        assertTrue(value.equals(String.valueOf(ran)) || value.equals(null) );
                    }

                    long end = System.currentTimeMillis();

                    System.out.println("cache LRU(size" + size + ") " + " put ratio: " + percentage/100.0 + " Processing time: " + (end - start) + "ms");
                    server.close();
                }
            }

            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
            System.out.println("LRU failed " + e);
        }

    }

    public void testCacheNone() {
        try {
            System.out.println("With Total (put, get) operations of " + testSize);

            for (int size = 200; size <= testSize; size = size + 200) {
                for (int percentage = 20; percentage < 100; percentage = percentage + 20) {

                    KVServer server = new KVServer(30000, size, "None");
                    server.clearStorage();

                    for (int i = 1; i <= testSize; i++) {
                        server.putKV("P-" + Integer.toString(i), Integer.toString(i));
                    }

                    long start = System.currentTimeMillis();



                    for (int i = 1; i <= testSize * ((float) percentage / (float) testSize); i++) {
                        int ran = new Random().nextInt(testSize-1)+1;
                        server.putKV("P-" + Integer.toString(ran), Integer.toString(ran));

                    }

                    for (int i = 1; i <= testSize * (1 - ((float) percentage / (float) testSize)); i++) {
                        int ran = new Random().nextInt(testSize-1)+1;
                        String value = server.getKV("P-" + Integer.toString(ran));
                        assertTrue(value.equals(String.valueOf(ran)) || value.equals(null) );
                    }

                    long end = System.currentTimeMillis();

                    System.out.println("None cache " + " put ratio: " + percentage/100.0 + " Processing time: " + (end - start) + "ms");
                    server.close();
                }
            }
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
            System.out.println("None cache failed " + e);
        }

    }
    public void testCacheLFU() {
        try {
            System.out.println("With Total (put, get) operations of " + testSize);

            for (int size = 200; size <= testSize; size = size + 200) {
                for (int percentage = 20; percentage < 100; percentage = percentage + 20) {

                    KVServer server = new KVServer(30000, size, "LFU");
                    server.clearStorage();

                    for (int i = 1; i <= testSize; i++) {
                        server.putKV("P-" + Integer.toString(i), Integer.toString(i));
                    }

                    long start = System.currentTimeMillis();



                    for (int i = 1; i <= testSize * ((float) percentage / (float) testSize); i++) {
                        int ran = new Random().nextInt(testSize-1)+1;
                        server.putKV("P-" + Integer.toString(ran), Integer.toString(ran));

                    }

                    for (int i = 1; i <= testSize * (1 - ((float) percentage / (float) testSize)); i++) {
                        int ran = new Random().nextInt(testSize-1)+1;
                        String value = server.getKV("P-" + Integer.toString(ran));
                        assertTrue(value.equals(String.valueOf(ran)) || value.equals(null) );
                    }

                    long end = System.currentTimeMillis();

                    System.out.println("cache LFU(size" + size + ") "+ " put ratio: " + percentage/100.0 + " Processing time: " + (end - start) + "ms");
                    server.close();
                }
            }
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
            System.out.println("None cache failed " + e);
        }

    }
    public void testCacheFIFO() {
        try {
            System.out.println("With Total (put, get) operations of " + testSize);

            for (int size = 200; size <= testSize; size = size + 200) {
                for (int percentage = 20; percentage < 100; percentage = percentage + 20) {

                    KVServer server = new KVServer(30000, size, "FIFO");
                    server.clearStorage();

                    for (int i = 1; i <= testSize; i++) {
                        server.putKV("P-" + Integer.toString(i), Integer.toString(i));
                    }

                    long start = System.currentTimeMillis();



                    for (int i = 1; i <= testSize * ((float) percentage / (float) testSize); i++) {
                        int ran = new Random().nextInt(testSize-1)+1;
                        server.putKV("P-" + Integer.toString(ran), Integer.toString(ran));

                    }

                    for (int i = 1; i <= testSize * (1 - ((float) percentage / (float) testSize)); i++) {
                        int ran = new Random().nextInt(testSize-1)+1;
                        String value = server.getKV("P-" + Integer.toString(ran));
                        assertTrue(value.equals(String.valueOf(ran)) || value.equals(null) );
                    }

                    long end = System.currentTimeMillis();

                    System.out.println("cache FIFO(size" + size + ") " + " put ratio: " + percentage/100.0 + " Processing time: " + (end - start) + "ms");
                    server.close();
                }
            }
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
            System.out.println("None cache failed " + e);
        }

    }
}

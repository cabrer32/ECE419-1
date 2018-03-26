package testing;

import app_kvECS.ECSClient;
import client.KVStore;
import common.messages.KVMessage;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Random;

public class PerformanceCacheNoneTest extends TestCase {

    private final int CACHE_SIZE = 50;
    private final String CACHE_STRATEGY = "None";

    private final int STORAGE_SERVER_MAX = 100;
    private final int KVCLIENT_MAX = 100;
    private final int WARM_UP_TIMES = 10;

    private final String ENRON_DATASET_PATH = "/Users/pannnnn/maildir/";

    private ECSClient ecsClient;

    @Before
    public void setUp() {
        ecsClient = new ECSClient("127.0.0.1",2181,"ecs.config");
        ecsClient.addNodes(5, CACHE_STRATEGY, CACHE_SIZE);
        ecsClient.start();
    }

    @After
    public void tearDown() {
        ecsClient.shutdown();
    }

    private void testEnron(ArrayList<KVStore> kvClients) {
        File file = new File(ENRON_DATASET_PATH);
        File[] files = file.listFiles();
        try {
            for (int i = 0; i<= kvClients.size(); i++) {
                putFromFiles(files[i], kvClients.get(i));
            }
            for (int i = 0; i<= kvClients.size(); i++) {
                getFromFiles(files[i], kvClients.get(i));
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("File resources used up!");
        }
    }

    private void putFromFiles(File file, KVStore client) {
        if (file.isDirectory()) {
            System.out.println("Searching directory ... " + file.getAbsoluteFile());
            for (File temp : file.listFiles()) {
                if (temp.isDirectory()) {
                    putFromFiles(temp, client);
                } else {
                    try {
                        String value = new String(Files.readAllBytes(temp.toPath()));
                        client.put(temp.getPath().substring(0,20), value.substring(0,1000));
                    } catch (IOException e) {
                        System.out.println("Read file " + temp.getAbsoluteFile() + "failed ... ");
                    }
                }
            }
        }
    }

    private void getFromFiles(File file, KVStore client) {
        if (file.isDirectory()) {
            System.out.println("Searching directory ... " + file.getAbsoluteFile());
            for (File temp : file.listFiles()) {
                if (temp.isDirectory()) {
                    getFromFiles(temp, client);
                } else {
                    try {
                        client.get(temp.getPath().substring(0,20));
                    } catch (IOException e) {
                        System.out.println("Read file " + temp.getAbsoluteFile() + "failed ... ");
                    }
                }
            }
        }
    }

    @Test
    public void testCacheNone() {
        Random rand = new Random();
        KVStore kvClient;

        try {
            System.out.println("================= " +  CACHE_STRATEGY + " | " + CACHE_SIZE + " =================");
            System.out.println();

//            for (int kvServerNum = 5; kvServerNum < STORAGE_SERVER_MAX; kvServerNum += 5) {
            ArrayList<KVStore> KVClients = new ArrayList<>();
            for (int kvClientNum = 5; kvClientNum < KVCLIENT_MAX; kvClientNum += 5) {

                System.out.println("Server Number: " + 5 + " | " +
                        "Client Number: " + kvClientNum);
                for (int i = 0; i < 5; i++) {
                    kvClient = new KVStore("localhost", 50007);
                    kvClient.connect();
                    // warm up the storage service, try to hit all servers.
                    for (int j = 0; j <= WARM_UP_TIMES; j++) {
                        int ran = rand.nextInt();
                        kvClient.put("WARM-UP-" + Integer.toString(ran), Integer.toString(ran));
                        kvClient.put("WARM-UP-" + Integer.toString(ran), "");
                    }
                    KVClients.add(kvClient);
                }

                long start = System.currentTimeMillis();

                testEnron(KVClients);

                long end = System.currentTimeMillis();

                System.out.println("Processing time: " + (end - start) + "ms");
                System.out.println();
            }
//                ecsClient.addNodes(5, CACHE_STRATEGY,CACHE_SIZE);
//            }
        } catch (Exception e) {
            assertTrue(false);
            System.out.println("None cache test failed " + e);
        }
    }
}
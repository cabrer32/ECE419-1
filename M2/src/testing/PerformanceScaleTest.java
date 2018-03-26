package testing;

import app_kvECS.ECSClient;
import client.KVStore;
import common.messages.KVMessage;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

public class PerformanceScaleTest extends TestCase {

    private final int STORAGE_SERVER_MAX = 100;
    private final int CACHE_SIZE_MAX = 500;
    private final int KVCLIENT_MAX = 100;
    private final int WARM_UP_TIMES = 30;

    private final String ENRON_DATASET_PATH = "/Users/pannnnn/maildir/";

    private ECSClient ecsClient;
    private ArrayList<KVStore> KVClients = new ArrayList<>();

    @Before
    public void setUp() {
        ecsClient = new ECSClient("127.0.0.1",2181,"ecs.config");
        ecsClient.addNodes(5, "None", 100);
        ecsClient.start();
    }

    @After
    public void tearDown() {
        ecsClient.shutdown();
    }

    private void testEnron(KVStore kvClient, int size) {
        File file = new File(ENRON_DATASET_PATH);
        File[] files = file.listFiles();
        try {
            for (int i = 0; i<= 10; i++) {
                putFromFiles(files[i], kvClient);
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
                        client.put(temp.getPath(), value);
                    } catch (IOException e) {
                        System.out.println("Read file " + temp.getAbsoluteFile() + "failed ... ");
                    }
                }
            }
        }
    }

    public void testScale() {
        Random rand = new Random();
        KVStore kvClient;
        KVMessage kvMessage;

        try {
            System.out.println("================= Scale Test =================");
            System.out.println();

            kvClient = new KVStore("localhost", 50000);
            kvClient.connect();
            for (int i = 1; i < 10; i++) {
                testEnron(kvClient, i);
                long start = System.currentTimeMillis();

                Set<String> removedNodes = new HashSet<String>();
                Collections.addAll(removedNodes, new String[]{"server6", "server1", "server7"});
                ecsClient.removeNodes(removedNodes);

                long end = System.currentTimeMillis();

                System.out.println("Processing time: " + (end - start) + "ms");
                System.out.println();
            }


        } catch (Exception e) {
            assertTrue(false);
            System.out.println("None cache test failed " + e);
        }
    }
}
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
import java.util.HashMap;
import java.util.Random;

public class PerformanceTest extends TestCase {

    private final int SERVER_NUM = 5;
    private final int CACHE_SIZE = 50;
    private final String CACHE_STRATEGY = "None";

    private final int KVCLIENT_MAX = 100;
    private final int WARM_UP_TIMES = 10;

//    private final String ENRON_DATASET_PATH = "/Users/pannnnn/maildir/watson-k/questar/";
    private final String ENRON_DATASET_PATH = "/Users/pannnnn/maildir/watson-k/capacity/";

    private ECSClient ecsClient;

    @Before
    public void setUp() {
        ecsClient = new ECSClient("127.0.0.1",2181,"ecs.config");
        ecsClient.addNodes(SERVER_NUM, CACHE_STRATEGY, CACHE_SIZE);
        ecsClient.start();
    }

    @After
    public void tearDown() {
        ecsClient.shutdown();
    }

    private void testEnron(ArrayList<KVStore> kvClients) {
        File file = new File(ENRON_DATASET_PATH);
//        File[] files = file.listFiles();
        try {
            for (int i = 0; i< kvClients.size(); i++) {
//                System.out.println("================= " + "Client " + (i+1) + " =================" );
                putFromFiles(file, kvClients.get(i));
            }
            for (int j = 0; j< kvClients.size(); j++) {
//                System.out.println("================= " + "Client " + (j+1) + " =================" );
                getFromFiles(file, kvClients.get(j));
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("File resources used up!");
        }
    }

    private HashMap<String, String> getFromFile(File file){
        for (File temp : file.listFiles()) {
            try {
                String value = new String(Files.readAllBytes(temp.toPath()));
                value = value.length() > 1000 ? value.substring(0,1000) : value;
                String key = temp.getPath();
                key = key.length() > 20 ? key.substring(0,20) : key;
                client.put(key, value);
            } catch (IOException e) {
                System.out.println("Read file " + temp.getAbsoluteFile() + "failed ... ");
            }
        }
    }

    private void putFromFiles(File file, KVStore client) {
        if (file.isDirectory()) {
//            System.out.println("Searching directory ... " + file.getAbsoluteFile());
            for (File temp : file.listFiles()) {
                if (temp.isDirectory()) {
                    putFromFiles(temp, client);
                } else {
                    try {
                        String value = new String(Files.readAllBytes(temp.toPath()));
                        value = value.length() > 1000 ? value.substring(0,1000) : value;
                        String key = temp.getPath();
                        key = key.length() > 20 ? key.substring(0,20) : key;
                        client.put(key, value);
                    } catch (IOException e) {
                        System.out.println("Read file " + temp.getAbsoluteFile() + "failed ... ");
                    }
                }
            }
        }
    }

//    void test() {
//        for (File temp : file.listFiles()) {
//            try {
//                String key = temp.getPath();
//                key = key.length() > 20 ? key.substring(0,20) : key;
//                client.get(key);
//            } catch (IOException e) {
//                System.out.println("Read file " + temp.getAbsoluteFile() + "failed ... ");
//            }
//        }
//    }

    private void getFromFiles(File file, KVStore client) {
        if (file.isDirectory()) {
//            System.out.println("Searching directory ... " + file.getAbsoluteFile());
            for (File temp : file.listFiles()) {
                if (temp.isDirectory()) {
                    getFromFiles(temp, client);
                } else {
                    try {
                        String key = temp.getPath();
                        key = key.length() > 20 ? key.substring(0,20) : key;
                        client.get(key);
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
            System.out.println(" Server Number | Cache Strategy | Cache Size ");
            System.out.println("================= " + SERVER_NUM + " | " + CACHE_STRATEGY + " | " + CACHE_SIZE + " =================");

            ArrayList<KVStore> KVClients = new ArrayList<>();
            for (int kvClientNum = 5; kvClientNum < KVCLIENT_MAX; kvClientNum += 5) {

                System.out.println("Server Number: " + 5 + " | " +
                        "Client Number: " + kvClientNum);
                for (int i = 0; i < 5; i++) {
                    kvClient = new KVStore("localhost", 50007);
                    kvClient.connect();
                    // warm up the storage service, try to hit all servers.
//                    for (int j = 0; j <= WARM_UP_TIMES; j++) {
//                        int ran = rand.nextInt();
//                        kvClient.put("WARM-UP-" + Integer.toString(ran), Integer.toString(ran));
//                        kvClient.put("WARM-UP-" + Integer.toString(ran), "");
//                    }
                    KVClients.add(kvClient);
                }

                long start = System.currentTimeMillis();

                testEnron(KVClients);

                long end = System.currentTimeMillis();

                System.out.println("Processing time: " + (end - start) + "ms");
                System.out.println();
            }
        } catch (Exception e) {
            System.out.println("None cache test failed " + e);
            assertTrue(false);
        }
    }
}
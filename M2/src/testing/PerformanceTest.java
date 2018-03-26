package testing;

import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import client.KVStore;
import common.messages.KVMessage;
import common.module.ClientThread;
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
import java.util.concurrent.CountDownLatch;

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
        ecsClient = new ECSClient("127.0.0.1", 2181, "ecs.config");
        ecsClient.addNodes(SERVER_NUM, CACHE_STRATEGY, CACHE_SIZE);
        ecsClient.start();
    }

    @After
    public void tearDown() {
        ecsClient.shutdown();
    }


    private HashMap<String, String> getFromFile(File file) {

        HashMap<String, String> map = new HashMap<>();

        for (File temp : file.listFiles()) {
            try {
                String value = new String(Files.readAllBytes(temp.toPath()));
                value = value.length() > 1000 ? value.substring(0, 1000) : value;
                String key = temp.getPath();
                key = key.length() > 20 ? key.substring(0, 20) : key;
                map.put(key, value);
            } catch (IOException e) {
                System.out.println("Read file " + temp.getAbsoluteFile() + "failed ... ");
            }
        }

        return map;
    }

    @Test
    public void testCacheNone() {
        Random rand = new Random();
        KVStore kvClient;

        try {
            System.out.println(" Server Number | Cache Strategy | Cache Size ");
            System.out.println("================= " + SERVER_NUM + " | " + CACHE_STRATEGY + " | " + CACHE_SIZE + " =================");

            File file = new File(ENRON_DATASET_PATH);
            HashMap<String, String> data = getFromFile(file);


            for (int kvClientNum = 5; kvClientNum < KVCLIENT_MAX; kvClientNum += 5) {


                ArrayList<KVStore> KVClients = new ArrayList<>();

                System.out.println("Server Number: " + SERVER_NUM + " | " + "Client Number: " + kvClientNum);
                for (int i = 0; i < kvClientNum; i++) {
                    kvClient = new KVStore("localhost", 50007);
                    kvClient.connect();
                    KVClients.add(kvClient);
                }
                long start = System.currentTimeMillis();

                CountDownLatch CL = new CountDownLatch(KVClients.size());


                for (KVStore client : KVClients) {
                    ClientThread ct = new ClientThread(data, CL, client);
                }

                try{
                    CL.await();
                }catch (Exception e){
                    System.out.println("error " + e);
                }



                long end = System.currentTimeMillis();
                System.out.println("Processing time: " + (end - start) + "ms");
                System.out.println();

                for (int i = 0; i < kvClientNum; i++) {
                    KVClients.get(i).disconnect();
                }
            }
        } catch (Exception e) {
            System.out.println("None cache test failed " + e);
            assertTrue(false);
        }
    }
}
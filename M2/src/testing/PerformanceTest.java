package testing;

import app_kvECS.ECSClient;
import client.KVStore;
import common.messages.KVMessage;
import junit.framework.TestCase;
import java.util.ArrayList;
import java.util.Random;

public class PerformanceTest extends TestCase {

    private final int STORAGE_SERVER_MAX = 100;
    private final int CACHE_SIZE_MAX = 500;
    private final int KVCLIENT_MAX = 100;
    private final int WARM_UP_TIMES = 30;
    // PUT should be greater or equal to GET
    private final int PUT_TIMES = 1000;
    private final int GET_TIMES = 1000;

    private ECSClient ecsClient;
    private ArrayList<KVStore> KVClients = new ArrayList<>();

    public void setUp() {
        ecsClient = new ECSClient("127.0.0.1",2181,"ecs.config");
        ecsClient.addNodes(3, "None", 100);
        ecsClient.start();
    }

    public void tearDown() {
        ecsClient.shutdown();
    }

    public void testCacheNone() {
        Random rand = new Random();
        KVStore kvClient;
        KVMessage kvMessage;
        try {
            System.out.println("================= NONE CACHE =================");
            System.out.println("With Total (put, get) operations of (" + PUT_TIMES + ", " + GET_TIMES + "):");

            for (int serverAdded = 0; serverAdded < STORAGE_SERVER_MAX - 3; serverAdded++) {
                for (int cacheSize = 50; cacheSize <= CACHE_SIZE_MAX; cacheSize += 50) {
                    for (int kvClientNum = 5, step = 5; kvClientNum <= KVCLIENT_MAX; kvClientNum += step) {

                        System.out.println("Server Number: " + (3 + serverAdded) + "|" +
                                "Cache Size: " + cacheSize + "|" +
                                "Client Number: " + kvClientNum);
                        for (int i = 0; i < step; i++) {
                            kvClient = new KVStore("localhost", 50000);
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

                        // test put and get request
                        for (KVStore client : KVClients) {
                            int[] records = new int[PUT_TIMES];
                            for (int i = 0; i < PUT_TIMES; i++) {
                                int ran = rand.nextInt();
                                records[i] = ran;
                                client.put("P-" + Integer.toString(ran), Integer.toString(ran));
                            }

                            for (int i = 0; i < GET_TIMES; i++) {
                                kvMessage = client.get("P-" + Integer.toString(records[i]));
                                assertTrue(kvMessage.getValue().equals(Integer.toString(records[i])));
                            }
                        }

                        long end = System.currentTimeMillis();

                        System.out.println("Processing time: " + (end - start) + "ms");
                        System.out.println();
                    }
                }
            }
        } catch (Exception e) {
            assertTrue(false);
            System.out.println("None cache test failed " + e);
        }
    }


        public void testMulticlients() {
        try {


            int total = 10000;

            for(int i = 1; i <= 100;i++){
                if(i!= 1 && i != 5 && i!= 20 && i!= 50 && i!= 100){
                    continue;
                }

                System.out.println("doing " + i);

                ArrayList<KVStore> clients = new ArrayList<>();

                for (int j = 1; j <= i; i++) {
                    KVStore kvClient = new KVStore("127.0.0.1", 50007);
                    kvClient.connect();
                    clients.add(kvClient);
                }

                for (KVStore client : clients) {
                    for(int d = 0; d < total/i; d++)
                    client.put(String.valueOf(d), String.valueOf(d + d));
                }

            }
        }
        catch (Exception e){
            System.out.println("error happens "+ e);

        }
    }

}
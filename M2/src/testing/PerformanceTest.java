package testing;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import common.module.ServerThread;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Queue;

public class PerformanceTest extends TestCase {

    private ECSClient ecsClient = null;
    private ArrayList<KVStore> kvClient = null;

    @BeforeClass
    public void setUp() throws  Exception{
        ecsClient = new ECSClient("127.0.0.1",2181,"ecs.config");
        ecsClient.addNodes(3, "FIFO", 100);
        ecsClient.start();
    }

    @AfterClass
    public void tearDown() {
        ecsClient.shutdown();
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
package testing;

import java.io.IOException;

import app_kvECS.ECSClient;
import common.module.ServerThread;
import ecs.ECS;
import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

public class AllTests {



    static {
        try {
            new LogSetup("logs/testing/test.log", Level.ALL);

            ECSClient ecsClient = new ECSClient("127.0.0.1",2181,"ecs.config");

            ecsClient.addNodes(2, "None", 100);

            ecsClient.start();

        } catch (IOException e) {
            System.out.println("Cannot initilize logger");
        }finally {
            System.out.println("final");
        }
    }


    public static Test suite() {
        TestSuite Suite = new TestSuite("Basic Storage ServerTest-Suite");

//        Suite.addTestSuite(ConnectionTest.class);
//        Suite.addTestSuite(KVStoreTest.class);
//        Suite.addTestSuite(KVServerTest.class);
//        Suite.addTestSuite(ECSNodeTest.class);
//        Suite.addTestSuite(ECSClientTest.class);
//        Suite.addTestSuite(CacheTest.class);
//        Suite.addTestSuite(PerformanceTest.class);
//


        return Suite;
    }





}
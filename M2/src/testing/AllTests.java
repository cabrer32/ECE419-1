package testing;

import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import app_kvServer.Cache.KVCache;
import common.messages.MetaData;
import common.module.CommunicationModule;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Level;
import java.io.IOException;

public class AllTests {



//    static {
//        try {
//            new LogSetup("logs/testing/test.log", Level.ALL);
//
//            ECSClient ecsClient = new ECSClient("127.0.0.1",2181,"ecs.config");
//
//            ecsClient.addNodes(2, "None", 100);
//
//            ecsClient.start();
//
//        } catch (IOException e) {
//            System.out.println("Cannot initilize logger");
//        }finally {
//            System.out.println("final");
//        }
    static {
        try {
            new LogSetup("logs/testing/test.log", Level.ALL);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Test suite() {
        TestSuite Suite = new TestSuite("Basic Storage ServerTest-Suite");
        // inherit from M1
        Suite.addTestSuite(ConnectionTest.class);
        Suite.addTestSuite(InteractionTest.class);

//         additional test case after M1
//        Suite.addTestSuite(KVClientTest.class);
        Suite.addTestSuite(ECSClientTest.class);
//        Suite.addTestSuite(KVCacheTest.class);
//        Suite.addTestSuite(KVServerTest.class);
//        Suite.addTestSuite(KVStoreTest.class);
//        Suite.addTestSuite(MetaDataTest.class);
//        Suite.addTestSuite(KVMessageTest.class);
//        Suite.addTestSuite(CommunicationModuleTest.class);
//        Suite.addTestSuite(ECSNodeTest.class);
//        Suite.addTestSuite(PerformanceTest.class);

        return Suite;
    }
}
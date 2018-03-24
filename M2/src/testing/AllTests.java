package testing;

import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Level;
import java.io.IOException;

public class AllTests {

<<<<<<< Updated upstream


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
=======
        static {
            try {
                new LogSetup("logs/testing/test.log", Level.ALL);
            } catch (IOException e) {
                System.out.println("Cannot initilize logger");
            }
>>>>>>> Stashed changes
        }

    public static Test suite() {
        TestSuite Suite = new TestSuite("Basic Storage ServerTest-Suite");

        Suite.addTestSuite(ECSClientTest.class);
//        Suite.addTestSuite(KVClientConnectionTest.class);
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
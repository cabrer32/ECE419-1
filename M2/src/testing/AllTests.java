package testing;

import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Level;

public class AllTests {

    static {
        try {
            new LogSetup("logs/testing/test.log", Level.INFO);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Test suite() {
        TestSuite Suite = new TestSuite("Basic Storage ServerTest-Suite");

        // inherit from M1
//        Suite.addTestSuite(ConnectionTest.class);
//        Suite.addTestSuite(InteractionTest.class);

        // additional test case after M1
//        Suite.addTestSuite(ECSClientTest.class);
//        Suite.addTestSuite(ECSClientInteractionTest.class);
//        Suite.addTestSuite(KVCacheTest.class);
//        Suite.addTestSuite(KVServerTest.class);
//        Suite.addTestSuite(MetaDataTest.class);
//        Suite.addTestSuite(ECSNodeTest.class);
        Suite.addTestSuite(PerformanceCacheNoneTest.class);
//        Suite.addTestSuite(PerformanceScale.class);

        return Suite;
    }
}
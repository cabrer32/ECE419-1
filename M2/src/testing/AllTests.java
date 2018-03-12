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


        } catch (IOException e) {
            System.out.println("Cannot initilize logger");
        }
    }


    public static Test suite() {
        TestSuite Suite = new TestSuite("Basic Storage ServerTest-Suite");

        Suite.addTestSuite(KVServerTest.class);
        Suite.addTestSuite(KVStoreTest.class);
        Suite.addTestSuite(ECSNodeTest.class);
        Suite.addTestSuite(ECSClientTest.class);

        return Suite;
    }

}
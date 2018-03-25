package testing;

import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.ArrayList;

// Some of the interface of KVStore has been tested in Connection Test.
// In this file, we only test the interface or test cases we added
public class KVStoreTest extends TestCase {

    //TO test multiple clients
    @Test
    public void testMultiClients() {
        Exception ex = null;

        ArrayList<KVStore> clients = new ArrayList<>();

        try {


            for (int i = 0; i <= 5; i++) {
                KVStore kvClient = new KVStore("localhost", 50007);
                kvClient.connect();
                assertTrue("KVClient is not connected!", kvClient.isConnected());
                clients.add(kvClient);
            }

            for (KVStore client : clients) {
                client.put("hi", "hi");
            }


            for (KVStore client : clients) {
                assertTrue("get operation failed ", client.get("hi").getValue()!=null);
            }

            for (KVStore client : clients) {
                client.disconnect();
            }

        } catch (Exception e) {
            ex = e;
        }
        assertNotNull(ex);
    }
}

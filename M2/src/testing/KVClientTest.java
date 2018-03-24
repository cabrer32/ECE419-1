package testing;

import client.KVStore;
import junit.framework.TestCase;

public class KVClientTest extends TestCase {

//Test if kvclient can be connected successfully
    public void testConnectionSuccess() {

        Exception ex = null;

        KVStore kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }

    //Test the interface get_store()
    public void testGetStore() {

        KVStore kvClient = new KVStore("localhost", 50011);
        assertNotNull("addNodes() failed.", kvClient);
    }


}

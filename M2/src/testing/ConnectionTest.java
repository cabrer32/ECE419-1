package testing;

import java.net.UnknownHostException;
import java.util.ArrayList;

import app_kvServer.KVServer;
import client.KVStore;
import common.module.ServerThread;
import junit.framework.TestCase;


public class ConnectionTest extends TestCase {

    KVServer server = null;
    ServerThread serverThread = null;


    public void testConnectionSuccess() {

        Exception ex = null;

        KVStore kvClient = new KVStore("localhost", 50007);
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }


    public void testUnknownHost() {
        Exception ex = null;
        KVStore kvClient = new KVStore("unknown", 50007);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof UnknownHostException);
    }


    public void testIllegalPort() {
        Exception ex = null;
        KVStore kvClient = new KVStore("localhost", 123456789);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof IllegalArgumentException);
    }

    public void testMultiClients() {
        Exception ex = null;

        ArrayList<KVStore> clients = new ArrayList<>();

        try {


            for (int i = 0; i <= 5; i++) {
                KVStore kvClient = new KVStore("localhost", 50007);
                kvClient.connect();
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

    }


}
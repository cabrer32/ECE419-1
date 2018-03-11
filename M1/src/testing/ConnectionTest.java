package testing;

import java.net.UnknownHostException;
import java.util.ArrayList;

import client.KVStore;
import junit.framework.TestCase;


public class ConnectionTest extends TestCase {

	
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
	
	
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 50000);
		
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


			for(int i = 0; i <= 5; i++ ){
				KVStore kvClient = new KVStore("localhost", 123456789);
				kvClient.connect();
				clients.add(kvClient);
			}

			for(KVStore client : clients ){
				client.put("hi","hi");
			}


			for(KVStore client : clients ){
				assertTrue("get operation failed ",client.get("hi").getValue().equals("hi"));
			}

			for(KVStore client : clients ){
				client.disconnect();
			}

		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex instanceof IllegalArgumentException);
	}

	
	

	
}


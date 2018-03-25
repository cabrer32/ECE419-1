package testing;

import java.net.UnknownHostException;
import java.util.ArrayList;

import app_kvServer.KVServer;
import client.KVStore;

import common.module.ServerThread;
import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;


// Originally ConnectionTest
public class ConnectionTest extends TestCase {

	private KVServer kvServer = null;
	ServerThread thread = null;

	@BeforeClass
	public void setUp() {
		kvServer = new KVServer("test", "",0);
		kvServer.initKVServer(50000, 5, "FIFO");
		kvServer.clearStorage();
		thread = new ServerThread(kvServer);
		thread.start();
	}

	@AfterClass
	public void tearDown() {
		thread.interrupt();
		kvServer.close();
	}

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


	
}


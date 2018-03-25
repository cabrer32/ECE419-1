package testing;

import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;

import common.module.ServerThread;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;


// Originally ConnectionTest
public class ConnectionTest extends TestCase {

	private ECSClient ecsClient;

	@Before
	public void setUp() {
		ecsClient = new ECSClient("127.0.0.1",2181,"ecs.config");
		ecsClient.addNodes(3, "None", 100);
		ecsClient.start();
	}

	@After
	public void tearDown() {
		ecsClient.shutdown();
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


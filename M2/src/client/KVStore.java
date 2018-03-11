package client;

import app_kvClient.ClientSocketListener;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import common.messages.KVMessage;
import common.messages.Message;
import common.module.CommunicationModule;
import ecs.ECSNode;
import org.apache.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

public class KVStore implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private boolean running;
	private CommunicationModule communicationModule;
	private Gson gson;

	/**
	 * Initialize KVStore with address and port of KVServer
	 *
	 * @param address the address of the KVServer
	 * @param port    the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		communicationModule = new CommunicationModule(address, port);
		gson = new Gson();
		listeners = new HashSet<ClientSocketListener>();
	}

	public void addListener(ClientSocketListener listener) {
		this.listeners.add(listener);
	}

	public KVMessage sendMessage(KVMessage msgReq) throws IOException {
		String msgJsonReq = gson.toJson(msgReq);
		communicationModule.sendMessage(msgJsonReq);
		String msgJsonRes = communicationModule.receiveMessage();
		KVMessage msg = null;
		try {
			msg = gson.fromJson(msgJsonRes, Message.class);
		} catch (JsonSyntaxException e) {
			System.out.println(e.getMessage());
		}
		return msg;
	}

	@Override
	public void connect() throws IOException {
		// TODO Auto-generated method stub
		communicationModule.connect();
		communicationModule.setStream();
		String welcomeMsg = communicationModule.receiveMessage();
		for (ClientSocketListener listener : listeners) {
			listener.handleNewMessage(welcomeMsg);
		}
		logger.info("Connection established");
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		logger.info("try to close connection ...");
		try {
			communicationModule.disconnect();
			for (ClientSocketListener listener : listeners) {
				listener.handleStatus(ClientSocketListener.SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	@Override
	public boolean isConnected() {
		// TODO Auto-generated method stub
		return (communicationModule.getSocket() != null);
	}

	@Override
	public KVMessage put(String key, String value) throws IOException {
		// TODO Auto-generated method stub
		KVMessage msgReq = new Message(KVMessage.StatusType.PUT, key, value);
		KVMessage msgRes = sendMessage(msgReq);

		if (msgRes.getStatus().equals("SERVER_NOT_RESPONSIBLE")) ;
		{
			//diconnect the server
			if (isConnected()) {
				disconnect();
			}
			// find hash, and look for that has in latest metatable just received
			getHash(key);
		}
		if (msgRes.getStatus().equals("SERVER_STOPPED")) ;
		{
			//display error message
		}
		if (msgRes.getStatus().equals("SERVER_WRITE_LOCK ")) ;
		{
			// display error message
		}
		//get the hash
		return msgRes;
	}

	@Override
	public KVMessage get(String key) throws IOException {
		// TODO Auto-generated method stub
		KVMessage msgReq = new Message(KVMessage.StatusType.GET, key, "");
		KVMessage msgRes = sendMessage(msgReq);

		if (msgRes.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
			//diconnect the server
			if (isConnected()) {
				disconnect();
			}
			// find hash, and look for that has in latest metatable just received
			getHash(key);
		}
		if (msgRes.getStatus() == KVMessage.StatusType.SERVER_STOPPED) {
			//display error message
			System.out.println("SERVER_STOPPED");
			//diconnect the server
			if (isConnected()) {
				disconnect();
			}
		}
		if (msgRes.getStatus() == KVMessage.StatusType.SERVER_WRITE_LOCK) {
			// display error message
			System.out.println("SERVER_WRITE_LOCK");
			//diconnect the server
			if (isConnected()) {
				disconnect();
			}
		}
		//get the hash
		return msgRes;
	}

	public String getHash(String Key) throws IOException {
		String hashValue;
		try {
			byte[] data1 = Key.getBytes("UTF-8");
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(data1);
			byte[] digest = md.digest();
			hashValue = DatatypeConverter.printHexBinary(digest).toUpperCase();
			return hashValue;
		} catch (NoSuchAlgorithmException e) {
			System.out.println("Unable to hash the key");

		}
		return null;

	}
}

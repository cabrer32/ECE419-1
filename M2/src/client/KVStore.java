package client;

import app_kvClient.ClientSocketListener;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import common.messages.KVMessage;
import common.messages.Message;
import common.module.CommunicationModule;
import ecs.ECSNode;
import ecs.IECSNode;
import org.apache.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class KVStore implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	private static final String STARTING_HASH_VALUE = "00000000000000000000000000000000";
	private static final String ENDING_HASH_VALUE = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
	private Set<ClientSocketListener> listeners;
	private boolean running;
//	private CommunicationModule communicationModule;
	private HashMap<String, CommunicationModule> communicationModules;
	private Gson gson;
	private TreeSet<IECSNode> serverList;
	private String firstServerName;

	/**
	 * Initialize KVStore with address and port of KVServer
	 *
	 * @param address the address of the KVServer
	 * @param port    the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		serverList = null;
		communicationModules = new HashMap<>();
		firstServerName = "server8";
		communicationModules.put(firstServerName, new CommunicationModule(address, port));
		gson = new Gson();
		listeners = new HashSet<ClientSocketListener>();
	}

	public void addListener(ClientSocketListener listener) {
		this.listeners.add(listener);
	}

	public KVMessage sendMessage(CommunicationModule cm, KVMessage msgReq) throws IOException {
		String msgJsonReq = gson.toJson(msgReq);
		cm.sendMessage(msgJsonReq);
		String msgJsonRes = cm.receiveMessage();
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
		if(!communicationModules.isEmpty()) {
			CommunicationModule ci = communicationModules.values().iterator().next();
			ci.connect();
			ci.setStream();
			String welcomeMsg = ci.receiveMessage();
			for (ClientSocketListener listener : listeners) {
				listener.handleNewMessage(welcomeMsg);
			}
			logger.info("Connection established");
		}
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		logger.info("try to close connection ...");
		try {
			for (CommunicationModule ci : communicationModules.values()) {
				ci.disconnect();
			}
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
		boolean ifAnyConnected = false;
		for (CommunicationModule ci : communicationModules.values()) {
			ifAnyConnected = ifAnyConnected || (ci.getSocket() != null);
		}
		return ifAnyConnected;
	}

	@Override
	public KVMessage put(String key, String value) throws IOException {
		// TODO Auto-generated method stub
		KVMessage msgReq = null;
		KVMessage msgRes = null;
		while(true) {
			if (serverList == null) {
				CommunicationModule ci = communicationModules.get(firstServerName);
				msgReq = new Message(KVMessage.StatusType.PUT, key, value);
				msgRes = sendMessage(ci, msgReq);
			} else {
				String keyHashValue = getHashValue(key);
				IECSNode targetNode = null;
				for (IECSNode node : serverList) {
					if (((ECSNode) node).contains(keyHashValue)) {
						targetNode = node;
					}
				}
				CommunicationModule ci = communicationModules.get(targetNode.getNodeName());
				if(ci == null) {
					ci = new CommunicationModule(targetNode.getNodeHost(), targetNode.getNodePort());
					ci.connect();
					ci.setStream();
					communicationModules.put(targetNode.getNodeName(), ci);
					msgReq = new Message(KVMessage.StatusType.PUT, key, value);
					msgRes = sendMessage(ci, msgReq);
				} else {
					msgReq = new Message(KVMessage.StatusType.PUT, key, value);
					msgRes = sendMessage(ci, msgReq);
				}
			}
			if (msgReq != null) {
				if (msgRes.getStatus().equals("SERVER_NOT_RESPONSIBLE")) {
					serverList = gson.fromJson(msgRes.getValue(), new TypeToken<TreeSet<IECSNode>>(){}.getType());
				} else if (msgRes.getStatus().equals("SERVER_STOPPED")) {
					for (ClientSocketListener listener : listeners) {
						listener.handleNewMessage("Server stopped! Please try again later!");
					}
					break;
				} else if (msgRes.getStatus().equals("SERVER_WRITE_LOCK ")) {
					for (ClientSocketListener listener : listeners) {
						listener.handleNewMessage("Server is leaving or joining! Please try again later!");
					}
					break;
				} else {
					break;
				}
			}
		}
		return msgRes;
	}

	@Override
	public KVMessage get(String key) throws IOException {
		// TODO Auto-generated method stub
		KVMessage msgReq = null;
		KVMessage msgRes = null;
		while(true) {
			if (serverList == null) {
				CommunicationModule ci = communicationModules.get(firstServerName);
				msgReq = new Message(KVMessage.StatusType.GET, key, "");
				msgRes = sendMessage(ci, msgReq);
			} else {
				String keyHashValue = getHashValue(key);
				IECSNode targetNode = null;
				for (IECSNode node : serverList) {
					if (((ECSNode) node).contains(keyHashValue)) {
						targetNode = node;
					}
				}
				CommunicationModule ci = communicationModules.get(targetNode.getNodeName());
				if(ci == null) {
					ci = new CommunicationModule(targetNode.getNodeHost(), targetNode.getNodePort());
					ci.connect();
					ci.setStream();
					communicationModules.put(targetNode.getNodeName(), ci);
					msgReq = new Message(KVMessage.StatusType.GET, key, "");
					msgRes = sendMessage(ci, msgReq);
				} else {
					msgReq = new Message(KVMessage.StatusType.GET, key, "");
					msgRes = sendMessage(ci, msgReq);
				}
			}
			if (msgReq != null) {
				if (msgRes.getStatus().equals("SERVER_NOT_RESPONSIBLE")) {
					serverList = gson.fromJson(msgRes.getValue(), new TypeToken<TreeSet<IECSNode>>(){}.getType());
				} else if (msgRes.getStatus().equals("SERVER_STOPPED")) {
					for (ClientSocketListener listener : listeners) {
						listener.handleNewMessage("Server stopped! Please try again later!");
					}
					break;
				} else if (msgRes.getStatus().equals("SERVER_WRITE_LOCK ")) {
					logger.error("Something wrong here!");
					break;
				} else {
					break;
				}
			}
		}
		return msgRes;
	}

	public String getHashValue(String key) throws IOException {
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("MD5");
			md.update(key.getBytes());
			byte[] digest = md.digest();
			String hashValue = DatatypeConverter.printHexBinary(digest).toUpperCase();
			return hashValue;
		} catch (NoSuchAlgorithmException e) {
			logger.error("Unable to hash the key " + key + "!");
		}
		return null;
	}
}

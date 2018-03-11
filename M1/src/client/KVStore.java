package client;

import app_kvClient.ClientSocketListener;
import common.module.CommunicationModule;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.Message;
import org.apache.log4j.Logger;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.net.UnknownHostException;
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
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		communicationModule = new CommunicationModule(address, port);
		gson = new Gson();
		listeners = new HashSet<ClientSocketListener>();
	}

	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
	}

	@Override
	public void connect() throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		communicationModule.connect();
		communicationModule.setStream();
		String welcomeMsg = communicationModule.receiveMessage();
		for(ClientSocketListener listener : listeners) {
			listener.handleNewMessage(welcomeMsg);
		}
//		setRunning(true);
		logger.info("Connection established");
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
//		setRunning(false);
		logger.info("try to close connection ...");
		try {
			communicationModule.disconnect();
			for(ClientSocketListener listener : listeners) {
				listener.handleStatus(ClientSocketListener.SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	@Override
	public KVMessage put(String key, String value) throws IOException {
		// TODO Auto-generated method stub
		KVMessage msgReq = new Message(StatusType.PUT, key, value);
		KVMessage msgRes = sendMessage(msgReq);
		return msgRes;
	}

	@Override
	public KVMessage get(String key) throws IOException {
		// TODO Auto-generated method stub
		KVMessage msgReq = new Message(StatusType.GET, key, "");
		KVMessage msgRes = sendMessage(msgReq);
		return msgRes;
	}

	public KVMessage sendMessage(KVMessage msgReq) throws IOException{
		String msgJsonReq = gson.toJson(msgReq);
//		System.out.println(msgJsonReq);
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
}

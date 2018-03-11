package app_kvClient;

import common.messages.KVMessage;

public interface ClientSocketListener {

	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};

	public void handleNewMessage(String msg);

	public void handleNewMessage(KVMessage msg);
	
	public void handleStatus(SocketStatus status);
}

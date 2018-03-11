package common.messages;

import java.io.Serializable;

public class Message implements KVMessage, Serializable{

	public StatusType type;
	public String key;
	public String value;

	public Message(StatusType type, String key, String value) {
		this.type = type;
		this.key = key;
		this.value = value;
	}

	@Override
	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey() { return key; }

	@Override
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue() {
		return value;
	}
	
	@Override
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus() { return type; }

	public void setType(StatusType type) {
		this.type = type;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public void setValue(String value) {
		this.value = value;
	}
}


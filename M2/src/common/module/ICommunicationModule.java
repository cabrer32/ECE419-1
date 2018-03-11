package common.module;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public interface ICommunicationModule {

    /**
     * Method sends a KVMessage using this socket.
     * @param jsonBody the json body that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(String jsonBody) throws IOException;

    /**
     * Method receives a KVMessage from the server.
     * @return received KVMessage from the server
     * @throws IOException Exception for receiving failures
     */
    public String receiveMessage() throws IOException;

    /**
     * Establishes a connection to the KV Server.
     *
     * @throws Exception
     *             if connection could not be established.
     */
    public void connect() throws Exception;

    /**
     * set the input/output stream from the socket.
     * @throws Exception
     *             if streams could not be set.
     */
    public void setStream() throws Exception;

    /**
     * disconnects the client from the currently connected server.
     */
    public void disconnect() throws Exception;

    /**
     * get inputsteam from the currently connected socket
     */
    public InputStream getInputStream();

    /**
     * get outputstream from the currently connected socket
     */
    public OutputStream getOutputStream();

    /**
     * get socket of the current connection
     */
    public Socket getSocket();
}

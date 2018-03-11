package common.module;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class CommunicationModule implements ICommunicationModule {
    private Logger logger = Logger.getRootLogger();

    private String address;
    private int port;
    private Socket socket;
    private OutputStream output;
    private InputStream input;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;


    public CommunicationModule(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public CommunicationModule(Socket clientSocket) {
        this.socket = clientSocket;
    }

    @Override
    public void sendMessage(String jsonBody) throws IOException {
        byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
        byte[] jsonBytes = jsonBody.getBytes();
        byte[] msgBytes = new byte[jsonBytes.length + ctrBytes.length];
        System.arraycopy(jsonBytes, 0, msgBytes, 0, jsonBytes.length);
        System.arraycopy(ctrBytes, 0, msgBytes, jsonBytes.length, ctrBytes.length);
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("Send message:\t '" + jsonBody + "'");
    }

    @Override
    public String receiveMessage() throws IOException {

        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;


        //	Check if stream is closed (read returns -1)
		if (read == -1){
			throw new IOException();
		}


        while(read != 13 && reading) {/* carriage return */
        /* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

        /* only read valid characters, i.e. letters and numbers */
            if((read > 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }

        /* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

        /* read next char from stream */
            read = (byte) input.read();
        }

        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;
        String msg = new String(msgBytes);

        /* build final String */
        logger.info("Receive message:\t '" + msg + "'");
        return msg;
    }

    @Override
    public void connect() throws UnknownHostException, IOException {
        socket = new Socket(address, port);
    }

    @Override
    public void setStream() throws UnknownHostException, IOException{
        try {
            output = socket.getOutputStream();
            input = socket.getInputStream();
        } catch (IOException ioe) {
            disconnect();
            logger.error("Connection could not be established!");
            throw ioe;
        }
    }

    @Override
    public synchronized void disconnect() throws IOException {
        logger.info("tearing down the connection ...");
        if (socket != null) {
            input.close();
            output.close();
            socket.close();
            socket = null;
            logger.info("connection closed!");
        }
    }

    @Override
    public InputStream getInputStream() {
        return input;
    }

    @Override
    public OutputStream getOutputStream() {
        return output;
    }

    @Override
    public Socket getSocket(){return socket;}
}

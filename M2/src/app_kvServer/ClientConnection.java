package app_kvServer;


import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import common.messages.KVMessage;
import common.messages.Message;
import common.messages.MetaData;
import common.module.CommunicationModule;
import ecs.ECSNode;
import org.apache.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.ArrayList;


/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

    private static Logger logger = Logger.getRootLogger();

    private CommunicationModule communicationModule;
    private Gson gson;

    private KVServer server;
    private boolean isOpen;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(Socket clientSocket, KVServer server) {
        communicationModule = new CommunicationModule(clientSocket);
        gson = new Gson();
        this.server = server;
        isOpen = true;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {

            communicationModule.setStream();

            communicationModule.sendMessage(
                    "Connection to KVServer B9 established: "
                            + server.getHostname() + "/"
                            + server.getPort());

            while (isOpen) {

                try {
                    String Msg = communicationModule.receiveMessage();

                    KVServer.KVServerState serverState = server.getState();

                    KVMessage response = new Message(KVMessage.StatusType.PUT_ERROR, "", "");


                    if (serverState == KVServer.KVServerState.STOPPED)

                        response = new Message(KVMessage.StatusType.SERVER_STOPPED, "", "");

                    else if (serverState == KVServer.KVServerState.LOCKED)

                        response = new Message(KVMessage.StatusType.SERVER_WRITE_LOCK, "", "");

                    else {
                        //parse the msg into json
                        KVMessage msg = gson.fromJson(Msg, Message.class);

                        //check msg if valid
                        switch (msg.getStatus()) {
                            case GET:
                                try {
                                    response = get(msg.getKey());
                                } catch (Exception e) {
                                    logger.error("Error! Unable to execute GET operation " + e);
                                    response = new Message(KVMessage.StatusType.GET_ERROR, msg.getKey(), msg.getValue());
                                }
                                break;
                            case PUT:
                                try {
                                    response = put(msg.getKey(), msg.getValue());
                                } catch (Exception e) {
                                    logger.error("Error! Unable to execute PUT operation " + e);
                                    response = new Message(KVMessage.StatusType.PUT_ERROR, msg.getKey(), msg.getValue());
                                }
                                break;
                            default:
                                break;
                        }
                    }

                    communicationModule.sendMessage(gson.toJson(response));
                } catch (JsonSyntaxException e) {
                    logger.error("Invalid Message syntax " + e.getMessage());
                /* connection either terminated by the client or lost due to
                 * network problems*/
                } catch (IOException ioe) {
                    logger.info("Connection lost!");
                    isOpen = false;
                }
            }
        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);
        } finally {
            try {
                logger.error("Disconnecting ClientConnection !");
                if (communicationModule.getSocket() != null)
                    communicationModule.disconnect();
            } catch (IOException e) {
                logger.error("Unable to close connection!");
            }
        }
    }

    public void stop() {
        isOpen = false;
        try {
            communicationModule.disconnect();
        } catch (IOException e) {
            logger.error("Unable to close connection!");
        }

    }

    private boolean compare(String key){

        try {

            ECSNode node = (ECSNode) server.meta.getServerByKey(key);
            return node.getNodeName().equals(server.getName());

        }catch(Exception e){

            logger.error("Cannot convert md5 " + e);

        }
        return false;
    }

    private KVMessage get(String key) throws Exception {
        if (key.equals("") || key.contains(" ") || key.length() > 20) {
            return new Message(KVMessage.StatusType.GET_ERROR, key, "");
        }

        if(!compare(key)){
            return new Message(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, key, MetaData.MetaToJson("N",server.getMetaData()));
        }

        String value = server.getKV(key);
        if (value == null)
            return new Message(KVMessage.StatusType.GET_ERROR, key, "");
        else
            return new Message(KVMessage.StatusType.GET_SUCCESS, key, value);
    }

    private KVMessage put(String key, String value) throws Exception {
        if (key.equals("") || key.contains(" ") || key.length() > 20 || (value != null && value.length() > 120000)) {
            return new Message(KVMessage.StatusType.PUT_ERROR, key, value);
        }

        if(!compare(key)){
            return new Message(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, key, MetaData.MetaToJson("N",server.getMetaData()));
        }

        //case when deleting
        if (value == null || value.equals("")) {
            if (server.inStorage(key)) {
                server.putKV(key, null);
                return new Message(KVMessage.StatusType.DELETE_SUCCESS, key, value);
            }
            return new Message(KVMessage.StatusType.DELETE_ERROR, key, value);
        }

        //case when update
        if (server.inStorage(key)) {
            server.putKV(key, value);
            return new Message(KVMessage.StatusType.PUT_UPDATE, key, value);
        }

        //case when create
        server.putKV(key, value);
        return new Message(KVMessage.StatusType.PUT_SUCCESS, key, value);
    }

}
package client;

import app_kvClient.ClientSocketListener;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import common.messages.KVMessage;
import common.messages.Message;
import common.messages.MetaData;
import common.module.CommunicationModule;

import ecs.IECSNode;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class KVStore implements KVCommInterface {
    private Logger logger = Logger.getRootLogger();

    private HashMap<String, CommunicationModule> communicationModules;
    private Gson gson;
    private String firstServerName = "server8";

    private boolean loggedIn = false;
    private String username;

    private MetaData meta = null;

    private ClientSocketListener listener = null;


    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    public KVStore(String address, int port) {

        communicationModules = new HashMap<>();
        communicationModules.put(firstServerName, new CommunicationModule(address, port));

        gson = new Gson();
    }

    public void addListener(ClientSocketListener listener) {
        this.listener = listener;
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

    public KVMessage handleServerLogic(KVMessage msg) {

        KVMessage response = null;
        String serverName = null;
        try {
            if (meta == null) {
                serverName = firstServerName;
                response = sendMessage(communicationModules.get(firstServerName), msg);
            } else {

                IECSNode node = meta.getServerByKey(msg.getKey());


                if (node == null) {
                    node = meta.getServerRepo().first();
                }

                logger.debug("responsible server is " + node.getNodeName());

                serverName = node.getNodeName();
                if (communicationModules.get(node.getNodeName()) == null) {
                    CommunicationModule ci = new CommunicationModule(node.getNodeHost(), node.getNodePort());
                    connectTo(ci);
                    communicationModules.put(node.getNodeName(), ci);
                }

                response = sendMessage(communicationModules.get(node.getNodeName()), msg);

            }

            switch (response.getStatus()) {
                case SERVER_NOT_RESPONSIBLE:
                    meta = MetaData.JsonToMeta(response.getValue());
                    return handleServerLogic(msg);
            }
        } catch (IOException e) {
            communicationModules.remove(serverName);
            if (meta != null)
                meta.removeNode(serverName);
            logger.info("Responsible server is down, trying other servers. ");
        }

        return response;
    }


    private void connectTo(CommunicationModule ci) throws IOException {
        ci.connect();
        ci.setStream();
        String welcomeMsg = ci.receiveMessage();
        if (listener != null) {
            listener.handleNewMessage(welcomeMsg);
        }
    }


    @Override
    public void connect() throws IOException {

        if (!communicationModules.isEmpty()) {
            CommunicationModule ci = communicationModules.values().iterator().next();
            connectTo(ci);
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
            if (listener != null) {
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

        KVMessage msgReq = new Message(KVMessage.StatusType.PUT, this.username + key, value);


        KVMessage response = null;

        while (response == null)
            response = handleServerLogic(msgReq);

        if (listener != null) {
            listener.handleNewMessage(response.getStatus().toString());
        }
        return response;

    }

    @Override
    public KVMessage get(String key) throws IOException {
        KVMessage msgReq = new Message(KVMessage.StatusType.GET, this.username + key, "");

        KVMessage response = null;

        while (response == null)
            response = handleServerLogic(msgReq);

        if (listener != null) {
            listener.handleNewMessage(response.getStatus().toString() + " " + response.getValue());
        }
        return response;
    }

    public void logIn(String username) {
        this.username = username;
        this.loggedIn = true;
    }

    public boolean checkUserAccount(String username, String password) {
        try {
            KVMessage kvMessage = this.get(username);
            if (kvMessage.getValue().compareTo(password) == 0) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean createUserAccount(String username, String password) {
        try {
            KVMessage kvMessage = this.get(username);
            if (kvMessage.getStatus() == KVMessage.StatusType.GET_SUCCESS) {
                return false;
            } else {
                this.put(username, password);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
}

package client;

import app_kvClient.ClientSocketListener;
import app_kvServer.KVServer;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import common.messages.KVMessage;
import common.messages.Message;
import common.messages.MetaData;
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

    private HashMap<String, CommunicationModule> communicationModules;
    private Gson gson;
    private String firstServerName = "server8";

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

    public KVMessage handleServerLogic(KVMessage msg) throws IOException {

        KVMessage response;

        if(meta == null){
            response = sendMessage(communicationModules.get(firstServerName), msg);
        }else{
            IECSNode node = meta.getServerByKey(msg.getKey());

            if(communicationModules.get(node.getNodeName()) == null){
                CommunicationModule ci = new CommunicationModule(node.getNodeHost(), node.getNodePort());
                connectTo(ci);
                communicationModules.put(node.getNodeName(), ci);
            }

            response = sendMessage(communicationModules.get(node.getNodeName()), msg);

        }

        switch(response.getStatus()){
            case SERVER_NOT_RESPONSIBLE:
                meta = MetaData.JsonToMeta(response.getValue());
                return handleServerLogic(msg);
        }

        return response;
    }


    private void connectTo(CommunicationModule ci) throws IOException{
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

        KVMessage msgReq = new Message(KVMessage.StatusType.PUT, key, value);

        KVMessage response = handleServerLogic(msgReq);
        if (listener != null) {
            listener.handleNewMessage(response.getStatus().toString());
        }
        return response;

    }

    @Override
    public KVMessage get(String key) throws IOException {
        KVMessage msgReq = new Message(KVMessage.StatusType.GET, key, "");

        KVMessage response = handleServerLogic(msgReq);
        if (listener != null) {
            listener.handleNewMessage(response.getStatus().toString());
        }
        return response;
    }

}

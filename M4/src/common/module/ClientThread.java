package common.module;

import app_kvServer.ClientConnection;
import client.KVStore;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class ClientThread implements Runnable {

    private HashMap<String,String> map = null;

    private  CountDownLatch sep = null;

    private KVStore client = null;

    public ClientThread(HashMap<String,String> map, CountDownLatch sep, KVStore client){
        this.map = map;
        this.sep = sep;
        this.client = client;
    }

    public void run(){

        for (String i: map.keySet()) {
            try{
                client.put(i, map.get(i));
            }catch (Exception e){
                System.out.println("Error");
            }
        }


        for (String i: map.keySet()) {
            try{
                client.get(i);
            }catch (Exception e){
                System.out.println("Error");
            }
        }
        sep.countDown();
    }
}

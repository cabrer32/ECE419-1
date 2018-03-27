package app_kvServer;

import java.util.HashMap;

public class DataMover implements Runnable {

    private KVServerWatcher zkWatcher = null;

    private HashMap<String, String> map;

    private KVServer server;

    public DataMover(KVServerWatcher zkWatcher, HashMap<String, String> map, KVServer server) {
        this.zkWatcher = zkWatcher;
        this.map = map;
        this.server = server;
    }


    @Override
    public void run() {

        for (String name : server.getReplicas()) {
            zkWatcher.moveData(map, name);
        }
    }
}

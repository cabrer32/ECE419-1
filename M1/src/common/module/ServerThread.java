package common.module;

import app_kvServer.KVServer;

public class ServerThread extends Thread {

    private KVServer kvServer;

    public ServerThread(KVServer kvServer) {
        this.kvServer = kvServer;
    }

    public void run() {
        kvServer.clearStorage();
        kvServer.run();
    }
}

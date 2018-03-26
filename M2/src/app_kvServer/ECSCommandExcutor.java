package app_kvServer;

import common.messages.MetaData;

import org.apache.log4j.Logger;
import sun.rmi.runtime.Log;

public class ECSCommandExcutor implements Runnable {

    private KVServerWatcher kvWatcher;
    private String KVname;
    private Logger logger;
    private KVServer kvServer;
    private MetaData meta;
    private String action;

    public ECSCommandExcutor(KVServer kvServer, Logger logger, MetaData meta, String action){
        this.kvServer = kvServer;
        this.kvWatcher = kvServer.getZkWatch();
        this.KVname = kvServer.getName();
        this.logger = logger;
        this.meta = meta;
        this.action = action;
    }


    public void run() {

        logger.info("#################### New command from ECS ####################");

        switch (action) {
            case "A":
                logger.info("--- Start Server ---");
                kvServer.start();
                kvWatcher.signalECS();
                break;
            case "B":
                logger.info("--- Stop Server ---");
                kvServer.stop();
                break;
            case "C":
                logger.info("--- Transfer data to new servers ---");
                kvWatcher.reRangeNewServers(meta);
                break;
            case "D":
                logger.info("--- Transfer data to new replica ---");
                kvWatcher.reRangeNewReplicas(meta);
                break;
            case "E":
                logger.info("--- Remove Server ---");
                kvWatcher.removeServer(meta);
                break;
            case "F":
                logger.info("--- Update to new meta data ---");
                kvServer.setMetaData(meta);
                kvServer.replicas = meta.getReplica(KVname);
                kvServer.predecessor = meta.getPredecessor(KVname);
                kvWatcher.signalECS();
                break;
            case "G":
                logger.info("--- Kill Server ---");
                kvServer.kill();
                break;
            default:
                logger.error("Cannot recognize the meta " + action);
        }

        logger.info("--- Done! ---");

    }
}

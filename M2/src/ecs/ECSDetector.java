package ecs;

import common.module.CommunicationModule;

import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Logger;

public class ECSDetector implements Runnable {

    private CommunicationModule ci = null;

    private Logger logger = null;

    private ECS ecs = null;

    private boolean running = true;

    private IECSNode node = null;

    public ECSDetector(Logger logger, ECS ecs, IECSNode node) {
        this.logger = logger;
        this.ecs = ecs;
        this.node = node;
        ci = new CommunicationModule(node.getNodeHost(), node.getNodePort());
    }


    public void run() {

        try {
            ci.connect();
            ci.setStream();
        } catch (IOException e) {
            logger.error("Cannot connect to kvServer " + node.getNodeName());
            return;
        }

        try {
            while (running) {
                ci.receiveMessage();
            }
        } catch (IOException e) {
            logger.debug("Connection closed " + node.getNodeName());

            if(running){
                logger.error("Failure detected. ");
                ecs.handleFailure(node);
                stop();
            }
        }
    }

    public void stop() {
        running = false;
        try{
            ci.disconnect();
        }catch(IOException e){
            logger.error("Cannot stop server detector " + node.getNodeName());
        }

    }


}

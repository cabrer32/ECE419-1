package common.messages;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import ecs.ECS;
import ecs.ECSNode;
import ecs.IECSNode;

import javax.xml.bind.DatatypeConverter;
import java.lang.reflect.Type;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class MetaData implements IMetaData {
    private TreeSet<IECSNode> serverRepo;


    public MetaData(TreeSet<IECSNode> serverRepo) {
        this.serverRepo = serverRepo;
        setHashRange();
    }

    public void setHashRange() {

        ArrayList<IECSNode> serverArray = new ArrayList<>(serverRepo);

        for(int i = 0; i< serverArray.size(); i++){

            ECSNode node = (ECSNode) serverArray.get(i);

            if(i == (serverArray.size() - 1))
                node.setEndingHashValue(serverArray.get(0).getNodeHashRange()[0]);

            else
                node.setEndingHashValue(serverArray.get(i + 1).getNodeHashRange()[0]);

        }

        serverRepo = new TreeSet<>(serverArray);
    }

    public TreeSet<IECSNode> getServerRepo() {
        return serverRepo;
    }


    @Override
    public String getPredecessor(String name) {

        ArrayList<IECSNode> serverArray = new ArrayList<>(serverRepo);

        for(int i = 0; i< serverArray.size() ; i++){
            if(serverArray.get(i).getNodeName().equals(name)){
             if(i == 0) return serverArray.get(serverArray.size() - 1).getNodeName();

             return serverArray.get(i-1).getNodeName();
            }
        }

        return null;
    }

    @Override
    public String getSuccessor(String name) {

        ArrayList<IECSNode> serverArray = new ArrayList<>(serverRepo);

        for(int i = 0; i< serverArray.size(); i++){
            if(serverArray.get(i).getNodeName().equals(name)){
                if(i == (serverArray.size() - 1)) return serverArray.get(0).getNodeName();

                return serverArray.get(i+1).getNodeName();
            }
        }

        return null;
    }

    @Override
    public String[] getHashRange(String name) {
        String[] hashRange = {};
        for (IECSNode node : serverRepo) {
            if (node.getNodeName().equals(name)) {
                hashRange = node.getNodeHashRange();
            }
        }
        return hashRange;
    }

    @Override
    public ArrayList<IECSNode> getServerBetween(String predecessor, String successor) {

        ECSNode suc = (ECSNode) this.getNode(successor);

        ECSNode pre = (ECSNode) this.getNode(predecessor);

        int flag = suc.compareTo(pre);

        if (flag == 0)
            return null;

        ArrayList<IECSNode> nodes = new ArrayList<>();

        Iterator itr = serverRepo.iterator();

        while (itr.hasNext()) {

            ECSNode node = (ECSNode) itr.next();

            if (((flag > 0) && (node.compareTo(pre) > 0) && (node.compareTo(suc) < 0)) ||
                    (((flag < 0) && (node.compareTo(pre) > 0) || (node.compareTo(suc) < 0)))) {
                nodes.add(node);
            }
        }

        return nodes;
    }

    @Override
    public IECSNode getNode(String name) {
        for (IECSNode node : serverRepo) {
            if (node.getNodeName().equals(name)) {
                return node;
            }
        }
        return null;
    }

    @Override
    public void addNode(IECSNode node) {
        serverRepo.add(node);
    }


    @Override
    public IECSNode removeNode(String name) {
        Iterator iter = serverRepo.iterator();
        IECSNode node;
        while (iter.hasNext()) {
            node = (IECSNode) iter.next();
            if (node.getNodeName().equals(name)) {
                iter.remove();
                return node;
            }
        }
        return null;
    }

    @Override
    public ArrayList<String> getReplica(String name){


        ArrayList<String> list = new ArrayList<>();

        list.add(getSuccessor(name));

        list.add(getSuccessor(list.get(0)));



return list;




    }



    public static String MetaToJson(MetaData meta) {
        try {
            Type listType = new TypeToken<TreeSet<ECSNode>>() {
            }.getType();

            String data = new Gson().toJson(meta.getServerRepo(), listType);

            return 1 + data;

        } catch (JsonSyntaxException e) {
            System.out.println("Invalid Message syntax " + e.getMessage());
        }
        return null;
    }

    public static MetaData JsonToMeta(String meta) {
        try {
            Type listType = new TypeToken<TreeSet<ECSNode>>() {
            }.getType();
            

            TreeSet<IECSNode> list = new Gson().fromJson(meta, listType);
            return new MetaData(list);

        } catch (JsonSyntaxException e) {
            System.out.println("Invalid Message syntax " + e.getMessage());
        }

        return null;
    }
}
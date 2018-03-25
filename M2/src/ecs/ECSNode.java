package ecs;

public class ECSNode implements IECSNode, Comparable<ECSNode>{
    private static final String STARTING_HASH_VALUE = "00000000000000000000000000000000";
    private static final String ENDING_HASH_VALUE = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";

    private String name;
    private String host;
    private String startingHashValue;
    private String endingHashValue;
    private String cacheStrategy;
    private int cachesize;
    private int port;


    public ECSNode (){}

    public ECSNode(String name, String host, int port, String startingHashValue) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.startingHashValue = startingHashValue;
    }

    @Override
    public String getNodeName() {
        return name;
    }

    @Override
    public String getNodeHost() {
        return host;
    }

    @Override
    public int getNodePort() {
        return port;
    }

    @Override
    public String[] getNodeHashRange() {
        return new String[] {this.getStartingHashValue(), this.getEndingHashValue()};
    }

    public String getStartingHashValue() {
        return startingHashValue;
    }

    public String getEndingHashValue() {
        return endingHashValue;
    }

    public String getCacheStrategy() {
        return cacheStrategy;
    }

    public int getCachesize() {
        return cachesize;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setStartingHashValue(String startingHashValue) {
        this.startingHashValue = startingHashValue;
    }

    public void setEndingHashValue(String endingHashValue) {
        this.endingHashValue = endingHashValue;
    }

    public void setCacheStrategy(String cacheStrategy) {
        this.cacheStrategy = cacheStrategy;
    }

    public void setCachesize(int cachesize) {
        this.cachesize = cachesize;
    }

    public boolean contains(String hashValue) {
        if ((startingHashValue.compareTo(endingHashValue) >= 0) &&
                ((hashValue.compareTo(startingHashValue) >= 0) ||
                (hashValue.compareTo(endingHashValue) < 0))) {
            return true;
        } else if ((startingHashValue.compareTo(endingHashValue) <= 0) &&
                (hashValue.compareTo(startingHashValue) >= 0) &&
                (hashValue.compareTo(endingHashValue) < 0)) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(ECSNode o)
    {
        return this.getStartingHashValue().compareTo(o.getStartingHashValue());
    }
}

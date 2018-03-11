package common.messages;

public class KVServerConfig {
    private String cacheStrategy;

    private String cacheSize;

    public KVServerConfig(String cacheStrategy, String cacheSize) {
        this.cacheStrategy = cacheStrategy;
        this.cacheSize = cacheSize;
    }

    public String getCacheStrategy ()
    {
        return cacheStrategy;
    }

    public void setCacheStrategy (String cacheStrategy)
    {
        this.cacheStrategy = cacheStrategy;
    }

    public String getCacheSize ()
    {
        return cacheSize;
    }

    public void setCacheSize (String cacheSize)
    {
        this.cacheSize = cacheSize;
    }
}

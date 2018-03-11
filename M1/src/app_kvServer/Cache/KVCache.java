package app_kvServer.Cache;

public interface KVCache {

    String getKV(String K);

    void putKV(String K, String V);

    void clear();

}

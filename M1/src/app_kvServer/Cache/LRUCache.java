package app_kvServer.Cache;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache extends LinkedHashMap<String, String> implements KVCache {

    private int cacheSize;

    public LRUCache(int cacheSize) {
        super(cacheSize+1, ((float) 1), true);
        this.cacheSize = cacheSize;
    }


    public String getKV(String K) {

        return super.get(K);
    }

    public void putKV(String K, String V) {

        if (V == null)
            super.remove(K);
        else
            super.put(K, V);

    }

    public void print(){
        System.out.println(Arrays.toString(this.entrySet().toArray()));
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
        return size() > cacheSize;
    }

    public void clear() {
        super.clear();
    }
}

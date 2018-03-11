package app_kvServer.Cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class FIFOCache extends LinkedHashMap<String,String> implements KVCache {

    private int cacheSize;

    public FIFOCache(int cacheSize) {
        super(cacheSize+1, ((float) 1), false);
        this.cacheSize = cacheSize;
    }


    public String getKV(String K){
        return super.get(K);
    }

    public void putKV(String K, String V){
        if (V == null || V.equals(""))
            super.remove(K);
        else
            super.put(K, V);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
        return size() > cacheSize;
    }
    public void clear(){super.clear();}


}

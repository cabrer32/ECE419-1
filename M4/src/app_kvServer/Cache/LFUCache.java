package app_kvServer.Cache;

import java.util.HashMap;
import java.util.LinkedHashSet;


public class LFUCache implements KVCache {

    private HashMap<String, String> KVs;
    private HashMap<String, Integer> counts;
    private HashMap<Integer, LinkedHashSet<String>> list;

    private int cacheSize;
    private int min = -1;


    public LFUCache(int capacity) {
        this.cacheSize = capacity;

        KVs = new HashMap<>();
        counts = new HashMap<>();
        list = new HashMap<>();
        list.put(1, new LinkedHashSet<String>());
    }

    public void clear(){
        KVs = new HashMap<>();
        counts = new HashMap<>();
        list = new HashMap<>();
        list.put(1, new LinkedHashSet<String>());
    }

    public String getKV(String key) {
        if (!KVs.containsKey(key))
            return null;
        int count = counts.get(key);
        counts.put(key, count + 1);
        list.get(count).remove(key);
        if (count == min && list.get(count).size() == 0)
            min++;
        if (!list.containsKey(count + 1))
            list.put(count + 1, new LinkedHashSet<String>());
        list.get(count + 1).add(key);
        return KVs.get(key);
    }

    public void putKV(String key, String value) {
        if (cacheSize <= 0)
            return;

        if (KVs.containsKey(key)) {

            if (value == null || value.equals("")) {
                removeKV(key);
                return;
            }

            KVs.put(key, value);

            //do a counter increase after put.
            getKV(key);
            return;
        }

        if (KVs.size() >= cacheSize) {
            String oldKey = list.get(min).iterator().next();
            removeKV(oldKey);
        }

        KVs.put(key, value);
        counts.put(key, 1);
        min = 1;
        list.get(1).add(key);
    }

    private void removeKV(String key) {
        KVs.remove(key);
        int count = counts.remove(key);
        list.get(count).remove(key);
    }
}
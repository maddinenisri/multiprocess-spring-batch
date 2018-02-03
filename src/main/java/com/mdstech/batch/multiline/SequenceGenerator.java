package com.mdstech.batch.multiline;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SequenceGenerator {
    private Map<String, Integer> sequenceHolder = new ConcurrentHashMap<>();

    public Integer getNextSequence(String key) {
//        String key = String.format("%s_%s", key1, key2);
        Integer value = 1;
        if(sequenceHolder.containsKey(key)) {
            value = sequenceHolder.get(key) + 1;
        }
        else { // Based on Key Change clear existig record
            sequenceHolder.clear();
        }
        sequenceHolder.put(key, value);
        return value;
    }
}

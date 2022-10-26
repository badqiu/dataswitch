package com.github.dataswitch.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

public class MapUtil {

	public static Map toMap(Object[] array, String... keys) {
		if (array == null)
			return new HashMap();
		Map m = new LinkedHashMap();
		for (int i = 0; i < keys.length; i++) {
			if (array.length == i) {
				break;
			}
			m.put(keys[i], array[i]);
		}
		return m;
	}
	
    public static Map mergeAllMap(List<Map> rows) {
    	if(CollectionUtils.isEmpty(rows)) {
    		return Collections.EMPTY_MAP;
    	}
    	
        Map result = new HashMap();
        for (Map row : rows) {
            result.putAll(row);
        }
        return result;
    }
	
    
    public static Map getDifferenceMap(Map mainMap, Map compareMap) {
        Map result = new HashMap();
        compareMap.forEach((key, value) -> {
            if (mainMap.containsKey(key)) {
                return;
            }
            result.put(key, value);
        });
        return result;
    }
}

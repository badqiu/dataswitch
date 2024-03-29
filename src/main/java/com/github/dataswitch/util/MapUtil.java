package com.github.dataswitch.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

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

	public static Map newMap(Object... args) {
		Map map = new HashMap();
		for(int i = 0; i < args.length; i+=2) {
			map.put(args[i], args[i+1]);
		}
		return map;
	}
	
	public static Map newLinkedMap(Object... args) {
		Map map = new LinkedHashMap();
		for(int i = 0; i < args.length; i+=2) {
			map.put(args[i], args[i+1]);
		}
		return map;
	}
	

//    public static Map mergeAllMap(List<Map> rows) {
//    	if(CollectionUtils.isEmpty(rows)) {
//    		return Collections.EMPTY_MAP;
//    	}
//    	
//        Map result = new HashMap();
//        for (Map row : rows) {
//            result.putAll(row);
//        }
//        return result;
//    }
    
	public static Map mergeAllMapWithNotNullValue(List<Map> rows) {
		return mergeAllMapWithNotNullValue(rows,new TreeMap());
	}
	
    public static Map mergeAllMapWithNotNullValue(List<Map> rows,Map collector) {
    	if(CollectionUtils.isEmpty(rows)) {
    		return Collections.EMPTY_MAP;
    	}
    	
        Map result = collector;
        for (Map row : rows) {
            putWithNotNullValue(result, row);
        }
        
        return result;
    }

	public static void putWithNotNullValue(Map collector, Map row) {
		row.forEach((key,value) -> {
			if(containsKeyAndNotNullValue(collector, key)) {
				return;
			}
			
			collector.put(key, value);
		});
	}

	public static boolean containsKeyAndNotNullValue(Map collector, Object key) {
		if(collector.containsKey(key)) {
			Object value = collector.get(key);
			if(value != null) {
				return true;
			}
		}
		return false;
	}
    
	
	public static Map<String, Object> keyToLowerCase(Map<String, Object> properties) {
		if (properties == null) {
			return Collections.emptyMap();
		}

		Map result = new LinkedHashMap(properties.size() * 2);
		properties.forEach((key, value) -> result.put(StringUtils.lowerCase(key), value));

		return result;
	}
    
	/**
	 * 返回key的差集
	 * @param mainMap
	 * @param compareMap
	 * @return
	 */
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
    
	/**
	 * 将所有key 转换为小写
	 * @param list
	 * @return
	 */
	public static List<Map<String, Object>> allMapKey2LowerCase(
			List<Map<String, Object>> list) {
		List<Map<String,Object>> result = new ArrayList(list.size());
		for(Map<String,Object> row : list) {
			Map newRow = new LinkedHashMap();
			for(Map.Entry<String, Object> entry : row.entrySet()) {
				newRow.put(StringUtils.lowerCase(entry.getKey()), entry.getValue());
			}
			result.add(newRow);
		}
		return result;
	}
}

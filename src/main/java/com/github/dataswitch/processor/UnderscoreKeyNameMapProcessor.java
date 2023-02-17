package com.github.dataswitch.processor;

import java.util.LinkedHashMap;
import java.util.Map;

import com.github.dataswitch.util.Util;

/**
 *  将key转换成下划线分隔的名称。  示例： userName => user_name转换
 * @author badqiu
 *
 */
public class UnderscoreKeyNameMapProcessor extends BaseProcessor{

	@Override
	protected Object processOne(Object row) throws Exception {
		if(row == null) return null;
		
		if(row instanceof Map) {
			return processMap((Map)row);
		}
		
		return row;
	}

	private Object processMap(Map<String,Object> row) {
		return keyUnderscoreName(row);
	}

	private Object keyUnderscoreName(Map<String, Object> row) {
		Map<String,Object> r = new LinkedHashMap();
		row.forEach((key,value) -> {
			r.put(Util.underscoreName(key), value);
		});
		return r;
	}

}

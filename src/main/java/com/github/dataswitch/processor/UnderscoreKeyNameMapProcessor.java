package com.github.dataswitch.processor;

import java.util.LinkedHashMap;
import java.util.Map;

import com.github.dataswitch.util.Util;

/**
 *  将key转换成下划线分隔的名称。  示例： userName => user_name转换
 * @author badqiu
 *
 */
public class UnderscoreKeyNameMapProcessor extends ProcessOneProcessor{

	@Override
	protected Map<String,Object> processOne(Map<String,Object> row) throws Exception {
		return keyUnderscoreName(row);
	}

	private Map<String, Object> keyUnderscoreName(Map<String, Object> row) {
		if(row == null) return null;
		
		Map<String,Object> r = new LinkedHashMap();
		row.forEach((key,value) -> {
			r.put(Util.underscoreName(key), value);
		});
		return r;
	}

}

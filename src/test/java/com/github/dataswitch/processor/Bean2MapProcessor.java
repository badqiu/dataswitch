package com.github.dataswitch.processor;

import java.util.HashMap;
import java.util.Map;

import com.github.dataswitch.util.BeanUtils;

public class Bean2MapProcessor extends ProcessOneProcessor{

	@Override
	protected Map<String,Object> processOne(Map<String,Object> row) throws Exception {
		if(row == null) return null;
		
		if(row instanceof Map) {
			return (Map)row;
		}
		
		if(row.getClass().isPrimitive()) {
			Map map = new HashMap();
			map.put("value", row);
			return map;
		}
		
		Map r = BeanUtils.describe(row);
		r.remove("class");
		return r;
	}

}

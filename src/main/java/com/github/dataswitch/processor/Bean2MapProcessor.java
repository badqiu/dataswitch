package com.github.dataswitch.processor;

import java.util.HashMap;
import java.util.Map;

import com.github.dataswitch.util.BeanUtils;

public class Bean2MapProcessor extends BaseProcessor{

	@Override
	protected Object processOne(Object row) throws Exception {
		if(row == null) return null;
		
		if(row instanceof Map) {
			return row;
		}
		
		if(row.getClass().isPrimitive()) {
			Map map = new HashMap();
			map.put("value", row);
			return map;
		}
		
		return BeanUtils.describe(row);
	}

}

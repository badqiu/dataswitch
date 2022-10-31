package com.github.dataswitch.processor;

import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;

public class Bean2MapProcessor extends BaseProcessor{

	@Override
	protected Object processOne(Object row) throws Exception {
		if(row == null) return null;
		
		if(row instanceof Map) {
			return row;
		}
		
		return BeanUtils.describe(row);
	}

}

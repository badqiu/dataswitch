package com.github.dataswitch.processor;

import java.util.Map;

import org.springframework.util.Assert;

import com.github.dataswitch.util.Util;

public class RemoveKeysProcessor extends BaseProcessor {
	private String keys = null;
	
	private String[] _keysArray = null;
	
	public String getKeys() {
		return keys;
	}

	public void setKeys(String keys) {
		this.keys = keys;
	}

	@Override
	protected Map<String,Object> processOne(Map<String,Object> row) throws Exception {
		if(row == null) return null;
		
		Map map = (Map)row;
		
		return processMap(map);
	}

	private Map<String,Object> processMap(Map map) {
		for(String key : _keysArray) {
			map.remove(key);
		}
		return map;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		
		Assert.hasText(keys,"keys must be not blank");
		_keysArray = Util.splitColumns(keys);
	}

}

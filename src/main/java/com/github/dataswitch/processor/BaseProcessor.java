package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.github.dataswitch.BaseObject;

public abstract class BaseProcessor extends BaseObject implements Processor{

	@Override
	public List<Map<String, Object>> process(List<Map<String, Object>> datas) throws Exception {
		if(datas == null || datas.isEmpty()) return Collections.EMPTY_LIST;
		
		List<Map<String, Object>> results = new ArrayList<Map<String, Object>>(datas.size());
		for(Map<String,Object> row : datas) {
			Map<String,Object> result = processOne(row);
			if(result != null) {
				results.add(result);
			}
		}
		return results;
	}

	protected abstract Map<String,Object> processOne(Map<String, Object> row) throws Exception;

}

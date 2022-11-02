package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.github.dataswitch.BaseObject;

public abstract class BaseProcessor extends BaseObject implements Processor{

	@Override
	public List<Object> process(List<Object> datas) throws Exception {
		if(datas == null || datas.isEmpty()) return Collections.EMPTY_LIST;
		
		List<Object> results = new ArrayList<Object>(datas.size());
		for(Object row : datas) {
			Object result = processOne(row);
			if(result != null) {
				results.add(result);
			}
		}
		return results;
	}

	protected abstract Object processOne(Object row) throws Exception;

}

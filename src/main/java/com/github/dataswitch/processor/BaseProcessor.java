package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class BaseProcessor implements Processor{

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

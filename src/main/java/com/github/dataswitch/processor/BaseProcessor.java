package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class BaseProcessor implements Processor{

	@Override
	public List<Object> process(List<Object> datas) throws Exception {
		if(datas == null || datas.isEmpty()) return Collections.EMPTY_LIST;
		
		List<Object> result = new ArrayList<Object>(datas.size());
		for(Object row : datas) {
			Object processedRow = processOne(row);
			if(processedRow != null) {
				result.add(row);
			}
		}
		return result;
	}

	protected abstract Object processOne(Object row);

}

package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.github.dataswitch.processor.Processor;

/**
 * 1行变多行的processor
 * 
 * @author badqiu
 *
 */
public class One2ManyRowProcessor implements Processor {

	@Override
	public List<Object> process(List<Object> datas) throws Exception {
		if(datas == null || datas.isEmpty()) return Collections.EMPTY_LIST;
		
		List<Object> results = new ArrayList<Object>(datas.size());
		for(Object row : datas) {
			Collection list = toList(row);
			if(list == null || list.isEmpty()) {
				continue;
			}
			results.addAll(list);
		}
		return results;
	}

	protected Collection toList(Object row) throws Exception {
		if(row == null) return null;
		
		if(row instanceof Collection) {
			return (Collection)row;
		}else if(row.getClass().isArray()) {
			Object[] array = (Object[])row;
			return Arrays.asList(array);
		}else {
			return Arrays.asList(row);
		}
	}

}

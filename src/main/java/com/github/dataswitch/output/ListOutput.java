package com.github.dataswitch.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

public class ListOutput implements Output{

	private List list = new ArrayList();
	
	public ListOutput() {
	}
	
	public ListOutput(List list) {
		this.list = list;
	}

	@Override
	public void write(List<Map<String, Object>> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		list.addAll(rows);
	}

	public List getList() {
		return list;
	}

}

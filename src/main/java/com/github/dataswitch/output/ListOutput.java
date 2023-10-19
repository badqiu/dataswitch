package com.github.dataswitch.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

public class ListOutput implements Output{

	private List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
	
	public ListOutput() {
	}
	
	public ListOutput(List<Map<String, Object>> list) {
		this.list = list;
	}

	@Override
	public void write(List<Map<String, Object>> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		list.addAll(rows);
	}

	public List<Map<String, Object>> getList() {
		return list;
	}

}

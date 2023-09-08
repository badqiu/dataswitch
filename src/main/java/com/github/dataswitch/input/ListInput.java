package com.github.dataswitch.input;

import java.util.Collections;
import java.util.List;

public class ListInput implements Input{

	private List list;
	private boolean read = false;
	
	public ListInput() {
	}
	
	public ListInput(List list) {
		this.list = list;
	}
	
	public List getList() {
		return list;
	}

	public void setList(List list) {
		this.list = list;
	}

	@Override
	public synchronized List<Object> read(int size) {
		if(read) return Collections.EMPTY_LIST;
		
		read = true;
		return list;
	}
	
}

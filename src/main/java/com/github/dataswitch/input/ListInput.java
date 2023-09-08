package com.github.dataswitch.input;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListInput<T> implements Input{

	private List<T> list = new ArrayList<T>();
	private boolean read = false;
	
	public ListInput() {
	}
	
	public ListInput(List<T> list) {
		this.list = list;
	}
	
	public List<T> getList() {
		return list;
	}

	public void setList(List<T> list) {
		this.list = list;
	}

	@Override
	public synchronized List<Object> read(int size) {
		if(read) return Collections.EMPTY_LIST;
		
		read = true;
		return (List)list;
	}
	
}

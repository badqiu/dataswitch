package com.github.dataswitch.output;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.BaseObject;

public abstract class BaseOutput extends BaseObject implements Output {

	@Override
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		for(Object row : rows) {
			writeObject(row);
		}
	}

	public abstract void writeObject(Object obj);

}

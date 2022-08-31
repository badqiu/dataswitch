package com.github.dataswitch.output;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.util.QueueProvider;

public class QueueOutput extends QueueProvider implements Output{

	@Override
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		try {
			getQueue().put(rows);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
}

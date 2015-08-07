package com.github.dataswitch.processor;

import java.util.List;

public interface Processor {

	public List<Object> process(List<Object> datas) throws Exception;
	
}

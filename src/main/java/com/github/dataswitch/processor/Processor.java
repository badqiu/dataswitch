package com.github.dataswitch.processor;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.github.dataswitch.Enabled;
import com.github.dataswitch.Openable;

public interface Processor extends Function<List<Object>, List<Object>>,AutoCloseable,Openable{

	public List<Object> process(List<Object> datas) throws Exception;
	
	@Override
	public default void open(Map<String, Object> params) throws Exception {
		Enabled.assertEnabled(this);
	}
	
	@Override
	public default void close() throws Exception {
	}
	
	@Override
	default List<Object> apply(List<Object> t) {
		try {
			return process(t);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}

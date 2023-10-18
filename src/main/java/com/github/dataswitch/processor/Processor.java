package com.github.dataswitch.processor;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.github.dataswitch.Enabled;
import com.github.dataswitch.Openable;

public interface Processor extends Function<List<Map<String, Object>>, List<Map<String, Object>>>,AutoCloseable,Openable{

	public List<Map<String, Object>> process(List<Map<String, Object>> datas) throws Exception;
	
	@Override
	public default void open(Map<String, Object> params) throws Exception {
		Enabled.assertEnabled(this);
	}
	
	@Override
	public default void close() throws Exception {
	}
	
	@Override
	default List<Map<String, Object>> apply(List<Map<String, Object>> t) {
		try {
			return process(t);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
